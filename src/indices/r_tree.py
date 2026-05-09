import struct
import math
import os
import sys
import time
import heapq
from pathlib import Path

_project_root = str(Path(__file__).resolve().parents[2])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.indices.base_index import BaseIndex
from src.buffer_manager import BufferManager

PAGE_SIZE = 4096
HEADER_FMT = "<iiii"
HEADER_SIZE = struct.calcsize(HEADER_FMT)
ENTRY_FMT = "<ddddii"
ENTRY_SIZE = struct.calcsize(ENTRY_FMT)
MAX_ENTRIES = (PAGE_SIZE - HEADER_SIZE) // ENTRY_SIZE
MIN_ENTRIES = max(2, MAX_ENTRIES // 2)

class RTreeNode:
    def __init__(self, page_id, is_leaf, parent_id=-1):
        self.page_id = page_id
        self.is_leaf = is_leaf
        self.parent_id = parent_id
        self.entries = []

    @classmethod
    def unpack(cls, raw):
        pid, is_leaf, num, parent_id = struct.unpack_from(HEADER_FMT, raw, 0)
        node = cls(pid, bool(is_leaf), parent_id)
        offset = HEADER_SIZE
        for _ in range(num):
            minx, miny, maxx, maxy, d1, d2 = struct.unpack_from(ENTRY_FMT, raw, offset)
            node.entries.append({
                "min_x": minx, "min_y": miny, "max_x": maxx, "max_y": maxy,
                "data1": d1, "data2": d2
            })
            offset += ENTRY_SIZE
        return node

    def pack(self):
        raw = bytearray(PAGE_SIZE)
        struct.pack_into(HEADER_FMT, raw, 0, self.page_id, int(self.is_leaf), len(self.entries), self.parent_id)
        offset = HEADER_SIZE
        for e in self.entries:
            struct.pack_into(ENTRY_FMT, raw, offset,
                             e["min_x"], e["min_y"], e["max_x"], e["max_y"],
                             e["data1"], e["data2"])
            offset += ENTRY_SIZE
        return bytes(raw)

    def mbr(self):
        if not self.entries:
            return (0.0, 0.0, 0.0, 0.0)
        return (
            min(e["min_x"] for e in self.entries),
            min(e["min_y"] for e in self.entries),
            max(e["max_x"] for e in self.entries),
            max(e["max_y"] for e in self.entries)
        )


class RTree(BaseIndex):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.filename = f"src/data/{table_name}_rtree.idx"
        os.makedirs(os.path.dirname(self.filename) or ".", exist_ok=True)
        self.buffer = BufferManager(self.filename, PAGE_SIZE)
        
        self.root_id = 1
        self.next_free_page = 2
        if os.path.exists(self.filename) and os.path.getsize(self.filename) >= PAGE_SIZE * 2:
            self._load_meta()
        else:
            self._initialize_tree()

    def _load_meta(self):
        raw = self.buffer.read_page(0)
        self.root_id, self.next_free_page = struct.unpack_from("<ii", raw, 0)

    def _save_meta(self):
        raw = bytearray(PAGE_SIZE)
        struct.pack_into("<ii", raw, 0, self.root_id, self.next_free_page)
        self.buffer.write_page(0, bytes(raw))

    def _initialize_tree(self):
        self.root_id = 1
        self.next_free_page = 2
        self._save_meta()
        root = RTreeNode(self.root_id, is_leaf=True)
        self._write_node(root)

    def _read_node(self, page_id):
        return RTreeNode.unpack(self.buffer.read_page(page_id))

    def _write_node(self, node):
        self.buffer.write_page(node.page_id, node.pack())

    def _next_page(self):
        pid = self.next_free_page
        self.next_free_page += 1
        self._save_meta()
        return pid

    def _area(self, mbr):
        return max(0.0, mbr[2] - mbr[0]) * max(0.0, mbr[3] - mbr[1])

    def _union(self, m1, m2):
        return (
            min(m1[0], m2[0]), min(m1[1], m2[1]),
            max(m1[2], m2[2]), max(m1[3], m2[3])
        )

    def _enlargement(self, mbr, new_mbr):
        return self._area(self._union(mbr, new_mbr)) - self._area(mbr)

    def _distance_point(self, p1, p2):
        dx = p1[0] - p2[0]
        dy = p1[1] - p2[1]
        return math.sqrt(dx * dx + dy * dy)

    def _min_dist_mbr(self, point, mbr):
        cx, cy = point
        minx, miny, maxx, maxy = mbr
        dx = max(0.0, minx - cx, cx - maxx)
        dy = max(0.0, miny - cy, cy - maxy)
        return math.sqrt(dx * dx + dy * dy)

    def _result(self, data, start_time):
        return self._format_result(
            data,
            self.buffer.get_io_cost(),
            round((time.time() - start_time) * 1000, 3)
        )

    def add(self, key, page_id_value, slot_id_value):
        start = time.time()
        self.buffer.reset_io_cost()
        
        x, y = float(key[0]), float(key[1])
        new_mbr = (x, y, x, y)
        new_entry = {
            "min_x": x, "min_y": y, "max_x": x, "max_y": y,
            "data1": page_id_value, "data2": slot_id_value
        }

        curr_id = self.root_id
        path = []
        while True:
            node = self._read_node(curr_id)
            path.append(node)
            if node.is_leaf:
                break
            best_e = None
            best_enl = float('inf')
            best_a = float('inf')
            for e in node.entries:
                e_mbr = (e["min_x"], e["min_y"], e["max_x"], e["max_y"])
                enl = self._enlargement(e_mbr, new_mbr)
                a = self._area(e_mbr)
                if enl < best_enl or (enl == best_enl and a < best_a):
                    best_enl = enl
                    best_a = a
                    best_e = e
            curr_id = best_e["data1"]

        leaf = path[-1]
        leaf.entries.append(new_entry)
        
        split_node = None
        if len(leaf.entries) > MAX_ENTRIES:
            split_node = self._split_node(leaf)
        else:
            self._write_node(leaf)
            if leaf.page_id == self.root_id:
                return self._result(["inserted"], start)

        self._adjust_tree(leaf, split_node)
        return self._result(["inserted"], start)

    def _adjust_tree(self, node, split_node):
        while node.page_id != self.root_id:
            parent = self._read_node(node.parent_id)
            n_mbr = node.mbr()
            
            for e in parent.entries:
                if e["data1"] == node.page_id:
                    e["min_x"], e["min_y"], e["max_x"], e["max_y"] = n_mbr
                    break
            
            if split_node:
                s_mbr = split_node.mbr()
                parent.entries.append({
                    "min_x": s_mbr[0], "min_y": s_mbr[1],
                    "max_x": s_mbr[2], "max_y": s_mbr[3],
                    "data1": split_node.page_id, "data2": 0
                })
                if len(parent.entries) > MAX_ENTRIES:
                    new_split = self._split_node(parent)
                    self._write_node(parent)
                    node = parent
                    split_node = new_split
                    continue
                else:
                    split_node = None
                    
            self._write_node(parent)
            node = parent

        if split_node:
            new_root_id = self._next_page()
            new_root = RTreeNode(new_root_id, is_leaf=False)
            r_mbr = node.mbr()
            s_mbr = split_node.mbr()
            new_root.entries.append({
                "min_x": r_mbr[0], "min_y": r_mbr[1], "max_x": r_mbr[2], "max_y": r_mbr[3],
                "data1": node.page_id, "data2": 0
            })
            new_root.entries.append({
                "min_x": s_mbr[0], "min_y": s_mbr[1], "max_x": s_mbr[2], "max_y": s_mbr[3],
                "data1": split_node.page_id, "data2": 0
            })
            node.parent_id = new_root_id
            split_node.parent_id = new_root_id
            self._write_node(node)
            self._write_node(split_node)
            self._write_node(new_root)
            self.root_id = new_root_id
            self._save_meta()
        else:
            self._write_node(node)

    def _split_node(self, node):
        new_id = self._next_page()
        new_node = RTreeNode(new_id, is_leaf=node.is_leaf, parent_id=node.parent_id)
        
        entries = node.entries
        max_d = -float('inf')
        s1, s2 = 0, 1
        for i in range(len(entries)):
            mi = (entries[i]["min_x"], entries[i]["min_y"], entries[i]["max_x"], entries[i]["max_y"])
            ai = self._area(mi)
            for j in range(i+1, len(entries)):
                mj = (entries[j]["min_x"], entries[j]["min_y"], entries[j]["max_x"], entries[j]["max_y"])
                d = self._area(self._union(mi, mj)) - ai - self._area(mj)
                if d > max_d:
                    max_d = d
                    s1, s2 = i, j
        
        g1, g2 = [entries[s1]], [entries[s2]]
        mbr1 = (entries[s1]["min_x"], entries[s1]["min_y"], entries[s1]["max_x"], entries[s1]["max_y"])
        mbr2 = (entries[s2]["min_x"], entries[s2]["min_y"], entries[s2]["max_x"], entries[s2]["max_y"])
        
        used = {s1, s2}
        
        for i, e in enumerate(entries):
            if i in used: continue
            e_mbr = (e["min_x"], e["min_y"], e["max_x"], e["max_y"])
            enl1 = self._enlargement(mbr1, e_mbr)
            enl2 = self._enlargement(mbr2, e_mbr)
            
            if enl1 < enl2:
                g1.append(e)
                mbr1 = self._union(mbr1, e_mbr)
            elif enl2 < enl1:
                g2.append(e)
                mbr2 = self._union(mbr2, e_mbr)
            else:
                if self._area(mbr1) < self._area(mbr2):
                    g1.append(e)
                    mbr1 = self._union(mbr1, e_mbr)
                else:
                    g2.append(e)
                    mbr2 = self._union(mbr2, e_mbr)

        node.entries = g1
        new_node.entries = g2
        
        if not new_node.is_leaf:
            for e in new_node.entries:
                child = self._read_node(e["data1"])
                child.parent_id = new_node.page_id
                self._write_node(child)
                
        self._write_node(node)
        self._write_node(new_node)
        return new_node

    def search(self, key):
        start = time.time()
        self.buffer.reset_io_cost()
        x, y = float(key[0]), float(key[1])
        results = []
        
        q = [self.root_id]
        while q:
            curr_id = q.pop()
            node = self._read_node(curr_id)
            for e in node.entries:
                if e["min_x"] <= x <= e["max_x"] and e["min_y"] <= y <= e["max_y"]:
                    if node.is_leaf:
                        if math.isclose(x, e["min_x"]) and math.isclose(y, e["min_y"]):
                            results.append({"page_id": e["data1"], "slot_id": e["data2"]})
                    else:
                        q.append(e["data1"])
        
        return self._result(results, start)

    def remove(self, key):
        start = time.time()
        self.buffer.reset_io_cost()
        x, y = float(key[0]), float(key[1])
        
        q = [self.root_id]
        removed = False
        while q and not removed:
            curr_id = q.pop()
            node = self._read_node(curr_id)
            for i, e in enumerate(node.entries):
                if e["min_x"] <= x <= e["max_x"] and e["min_y"] <= y <= e["max_y"]:
                    if node.is_leaf:
                        if math.isclose(x, e["min_x"]) and math.isclose(y, e["min_y"]):
                            node.entries.pop(i)
                            self._write_node(node)
                            self._adjust_tree(node, None)
                            removed = True
                            break
                    else:
                        q.append(e["data1"])
                        
        return self._result(["removed"] if removed else [], start)

    def range_search(self, begin_key, end_key):
        start = time.time()
        self.buffer.reset_io_cost()
        x1, y1 = float(begin_key[0]), float(begin_key[1])
        x2, y2 = float(end_key[0]), float(end_key[1])
        
        results = []
        q = [self.root_id]
        while q:
            curr_id = q.pop()
            node = self._read_node(curr_id)
            for e in node.entries:
                if not (e["max_x"] < x1 or e["min_x"] > x2 or e["max_y"] < y1 or e["min_y"] > y2):
                    if node.is_leaf:
                        results.append({"page_id": e["data1"], "slot_id": e["data2"]})
                    else:
                        q.append(e["data1"])
        return self._result(results, start)

    def range_search_spatial(self, point, radius):
        start = time.time()
        self.buffer.reset_io_cost()
        cx, cy = float(point[0]), float(point[1])
        r = float(radius)
        
        results = []
        q = [self.root_id]
        while q:
            curr_id = q.pop()
            node = self._read_node(curr_id)
            for e in node.entries:
                e_mbr = (e["min_x"], e["min_y"], e["max_x"], e["max_y"])
                if self._min_dist_mbr((cx, cy), e_mbr) <= r:
                    if node.is_leaf:
                        if self._distance_point((cx, cy), (e["min_x"], e["min_y"])) <= r:
                            results.append({"page_id": e["data1"], "slot_id": e["data2"]})
                    else:
                        q.append(e["data1"])
        return self._result(results, start)

    def knn(self, point, k):
        start = time.time()
        self.buffer.reset_io_cost()
        cx, cy = float(point[0]), float(point[1])
        
        results = []
        pq = []
        heapq.heappush(pq, (0.0, 0, "node", self.root_id))
        seq = 1
        
        while pq and len(results) < k:
            dist, _, typ, val = heapq.heappop(pq)
            if typ == "data":
                results.append(val)
            else:
                node = self._read_node(val)
                for e in node.entries:
                    if node.is_leaf:
                        d = self._distance_point((cx, cy), (e["min_x"], e["min_y"]))
                        heapq.heappush(pq, (d, seq, "data", {"page_id": e["data1"], "slot_id": e["data2"]}))
                        seq += 1
                    else:
                        e_mbr = (e["min_x"], e["min_y"], e["max_x"], e["max_y"])
                        d = self._min_dist_mbr((cx, cy), e_mbr)
                        heapq.heappush(pq, (d, seq, "node", e["data1"]))
                        seq += 1
                        
        return self._result(results, start)
