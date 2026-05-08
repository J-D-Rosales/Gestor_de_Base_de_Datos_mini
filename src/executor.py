from __future__ import annotations

import csv as _csv
import os
import struct
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any

_BASE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BASE)
for _p in (_PROJECT_ROOT, _BASE, os.path.join(_BASE, "indices")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from indices.bplus_tree import BPlusTreeIndex as _RealBPlusTree
    from indices.heap_file import HeapFile as _RealHeapFile
    from indices.sequential_index import SequentialIndex as _RealSequentialIndex
    from src.indices.extendible_hashing import ExtendibleHashing as _RealHash
    from src.indices.r_tree import RTree as _RealRTree
    _REAL_BACKEND = True
except Exception as _e:
    _REAL_BACKEND = False
    _IMPORT_ERROR = _e


_TYPE_TO_FORMAT = {
    "INT":     "i",
    "FLOAT":   "d",
    "VARCHAR": "50s",
    "STR":     "50s",
}

_AIRBNB_KEY_SIZE = {
    "name": 60, "city": 30, "room_type": 20,
}

def _record_format(columnas: list[dict]) -> str:
    return "=" + "".join(_TYPE_TO_FORMAT.get(c["tipo"], "50s") for c in columnas)

@dataclass
class IndexInfo:
    tipo: str
    rtree_cols: list | None = None
    instancia: Any = None


@dataclass
class TableMetadata:
    name: str
    columnas: list[dict]
    record_format: str
    record_size: int
    indices: dict[str, IndexInfo] = field(default_factory=dict)
    csv_path: str | None = None
    backend: str = "memory"

    def col_index(self, col_name: str) -> int:
        for i, c in enumerate(self.columnas):
            if c["nombre"] == col_name:
                return i
        return -1

    def data_columns(self) -> list[dict]:
        return [c for c in self.columnas if c.get("indice") != "RTREE"]

    def data_col_index(self, col_name: str) -> int:
        i = 0
        for c in self.columnas:
            if c.get("indice") == "RTREE":
                continue
            if c["nombre"] == col_name:
                return i
            i += 1
        return -1


class Catalog:
    def __init__(self):
        self._tables: dict[str, TableMetadata] = {}

    def register(self, meta: TableMetadata):
        self._tables[meta.name] = meta

    def get(self, name: str) -> TableMetadata | None:
        return self._tables.get(name.lower())

    def __contains__(self, name: str):
        return name.lower() in self._tables

    def names(self) -> list[str]:
        return list(self._tables.keys())

PAGE_CAPACITY = 4

class _MemorySequentialFile:
    def __init__(self, table: str):
        self.table = table
        self._pages: dict[int, dict[int, list]] = {}
        self._next_page = 0
        self._next_slot = 0
        self.last_disk_accesses = 0
        self.last_time_ms = 0.0

    def add(self, values):
        if self._next_slot >= PAGE_CAPACITY:
            self._next_page += 1
            self._next_slot = 0
        pid, sid = self._next_page, self._next_slot
        self._pages.setdefault(pid, {})[sid] = list(values)
        self._next_slot += 1
        return pid, sid

    def get(self, pid, sid):
        return self._pages.get(pid, {}).get(sid)

    def remove(self, pid, sid):
        page = self._pages.get(pid)
        if not page or sid not in page:
            return False
        del page[sid]
        return True


class _MemoryIndex:
    def __init__(self):
        self._map: dict[Any, tuple[int, int]] = {}
        self.last_disk_accesses = 0
        self.last_time_ms = 0.0

    def add(self, key, pid, sid):
        self._map[key] = (pid, sid)

    def search(self, key):
        return self._map.get(key)

    def remove(self, key):
        return self._map.pop(key, None) is not None

    def range_search(self, lo, hi):
        return [rid for k, rid in self._map.items() if lo <= k <= hi]


class _MetricsTracker:

    def __init__(self):
        self.last_disk_accesses = 0
        self.last_time_ms = 0.0

    def _capture(self, res):
        if isinstance(res, dict):
            self.last_disk_accesses = int(res.get("disk_accesses", 0) or 0)
            self.last_time_ms = float(res.get("execution_time_ms", 0.0) or 0.0)
            return res.get("data")
        self.last_disk_accesses = 0
        self.last_time_ms = 0.0
        return res


class _RealHeapAdapter(_MetricsTracker):

    def __init__(self, heap):
        super().__init__()
        self._heap = heap

    def _reset(self):
        self._heap.buffer.reset_io_cost()

    def _record(self, t0):
        self.last_disk_accesses = self._heap.buffer.get_io_cost()
        self.last_time_ms = round((time.time() - t0) * 1000, 3)

    def add(self, values):
        self._reset(); t0 = time.time()
        rid = self._heap.add(values)
        self._record(t0)
        return rid

    def get(self, pid, sid):
        self._reset(); t0 = time.time()
        v = self._heap.get(pid, sid)
        self._record(t0)
        return v

    def remove(self, pid, sid):
        self._reset(); t0 = time.time()
        ok = self._heap.remove(pid, sid)
        self._record(t0)
        return ok

    def iter_records(self):
        return self._heap.iter_records()


class _RealSequentialIndexAdapter(_MetricsTracker):

    def __init__(self, idx):
        super().__init__()
        self._idx = idx

    def add(self, key, pid, sid):
        res = self._idx.add(key, pid, sid)
        self._capture(res)

    def search(self, key):
        data = self._capture(self._idx.search(key))
        if not data:
            return None
        return tuple(data[0])

    def search_all(self, key):
        data = self._capture(self._idx.search(key))
        return [tuple(rid) for rid in (data or [])]

    def range_search(self, lo, hi):
        data = self._capture(self._idx.range_search(lo, hi))
        return [tuple(rid) for rid in (data or [])]

    def remove(self, key):
        self._capture(self._idx.remove(key))
        return True


class _RealBPlusTreeAdapter(_MetricsTracker):

    def __init__(self, tree):
        super().__init__()
        self._tree = tree

    def add(self, key, pid, sid):
        res = self._tree.add(key, pid, sid)
        self._capture(res)

    def search(self, key):
        data = self._capture(self._tree.search(key))
        if not data:
            return None
        return tuple(data[0])

    def search_all(self, key):
        data = self._capture(self._tree.search(key))
        return [tuple(rid) for rid in (data or [])]

    def remove(self, key):
        self._capture(self._tree.remove(key))
        return True

    def range_search(self, lo, hi):
        data = self._capture(self._tree.range_search(lo, hi))
        return [tuple(rid) for rid in (data or [])]


class _RealHashAdapter(_MetricsTracker):
    def __init__(self, h):
        super().__init__()
        self._h = h

    @staticmethod
    def _pack(pid, sid):
        return pid * 65536 + sid

    @staticmethod
    def _unpack(value):
        sid = value % 65536
        pid = (value - sid) // 65536
        return pid, sid

    def add(self, key, pid, sid):
        res = self._h.add(key, self._pack(pid, sid))
        self._capture(res)

    def search(self, key):
        data = self._capture(self._h.search(key))
        if data is None:
            return None
        return self._unpack(data)

    def search_all(self, key):
        data = self._capture(self._h.search_all(key))
        if not data:
            return []
        return [self._unpack(v) for v in data]

    def remove(self, key):
        try:
            res = self._h.remove(key)
            self._capture(res)
        except Exception:
            self.last_disk_accesses = 0
            self.last_time_ms = 0.0
        return True

    def range_search(self, lo, hi):
        # Hash no soporta búsqueda por rango eficientemente.
        self.last_disk_accesses = 0
        self.last_time_ms = 0.0
        return []


class _RealRTreeAdapter(_MetricsTracker):

    def __init__(self, rt):
        super().__init__()
        self._rt = rt

    @staticmethod
    def _to_rids(data):
        return [(item["page_id"], item["slot_id"]) for item in (data or [])]

    def add(self, key, pid, sid):
        res = self._rt.add(key, pid, sid)
        self._capture(res)

    def search(self, key):
        data = self._capture(self._rt.search(key))
        rids = self._to_rids(data)
        return rids[0] if rids else None

    def search_all(self, key):
        data = self._capture(self._rt.search(key))
        return self._to_rids(data)

    def range_search(self, lo, hi):
        data = self._capture(self._rt.range_search(lo, hi))
        return self._to_rids(data)

    def range_search_spatial(self, point, radius):
        data = self._capture(self._rt.range_search_spatial(point, radius))
        return self._to_rids(data)

    def knn(self, point, k):
        data = self._capture(self._rt.knn(point, k))
        return self._to_rids(data)

    def remove(self, key):
        self._capture(self._rt.remove(key))
        return True


class Executor:
    def __init__(self, data_dir: str | None = None):
        self.catalog = Catalog()
        self._storage: dict[str, Any] = {}
        self.data_dir = data_dir or os.path.join(_BASE, "data")
        os.makedirs(self.data_dir, exist_ok=True)
        self._lock = threading.Lock()

    def execute(self, parsed):
        if parsed is None:
            return {"status": "error", "msg": "AST vacío"}
        if isinstance(parsed, list):
            return [self._dispatch(s) for s in parsed]
        return self._dispatch(parsed)

    def _dispatch(self, stmt):
        if not isinstance(stmt, dict):
            return {"status": "error", "msg": f"Sentencia no reconocida: {stmt!r}"}
        op = stmt.get("tipo_operacion")
        handler = {
            "CREATE_TABLE":          self._create_table,
            "INSERT":                self._insert,
            "SELECT_POINT":          self._select_point,
            "SELECT_RANGE":          self._select_range,
            "SELECT_SPATIAL_RADIUS": self._select_spatial,
            "SELECT_SPATIAL_KNN":    self._select_spatial,
            "DELETE":                self._delete,
        }.get(op)
        if handler is None:
            return {"status": "error", "msg": f"Operación no soportada: {op}"}
        with self._lock:
            return handler(stmt)

    def _create_table(self, stmt):
        name = stmt["tabla"]
        cols = stmt["columnas"]
        fmt  = _record_format(cols)
        meta = TableMetadata(
            name=name,
            columnas=cols,
            record_format=fmt,
            record_size=struct.calcsize(fmt),
            csv_path=stmt.get("csv_path"),
        )

        if meta.csv_path and _REAL_BACKEND:
            self._init_real_backend(meta)
        else:
            self._init_memory_backend(meta)

        self.catalog.register(meta)
        return {
            "status": "ok",
            "op": "CREATE_TABLE",
            "tabla": name,
            "columnas": [c["nombre"] for c in cols],
            "formato": fmt,
            "record_size": meta.record_size,
            "indices": {k: v.tipo for k, v in meta.indices.items()},
            "backend": meta.backend,
        }

    def _init_memory_backend(self, meta: TableMetadata):
        for c in meta.columnas:
            if c.get("indice") in ("BTREE", "HASH", "RTREE", "SEQUENTIAL"):
                meta.indices[c["nombre"]] = IndexInfo(
                    tipo=c["indice"],
                    rtree_cols=c.get("rtree_cols"),
                    instancia=_MemoryIndex(),
                )
        self._storage[meta.name] = _MemorySequentialFile(meta.name)
        meta.backend = "memory"

    def _close_existing_table(self, name: str):
        prev = self.catalog.get(name)
        if prev is None:
            return
        # Cerrar HeapFile.
        storage = self._storage.get(name)
        if storage is not None:
            try:
                heap = getattr(storage, "_heap", None)
                if heap and hasattr(heap, "buffer"):
                    heap.buffer.close()
            except Exception:
                pass
        # Cerrar buffers de cada índice.
        for info in prev.indices.values():
            inst = info.instancia
            if inst is None:
                continue
            inner = (getattr(inst, "_tree", None) or getattr(inst, "_idx", None)
                     or getattr(inst, "_h", None) or getattr(inst, "_rt", None))
            if inner is None:
                continue
            for buf_attr in ("buffer", "main", "aux"):
                buf = getattr(inner, buf_attr, None)
                if buf is not None and hasattr(buf, "close"):
                    try:
                        buf.close()
                    except Exception:
                        pass

    def _init_real_backend(self, meta: TableMetadata):
        self._close_existing_table(meta.name)

        csv_path = meta.csv_path
        if not os.path.isabs(csv_path):
            csv_path = os.path.join(self.data_dir, csv_path)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"No se encontró el CSV: {csv_path}")

        table_dir = os.path.join(self.data_dir, "tables", meta.name)
        os.makedirs(table_dir, exist_ok=True)
        heap_path = os.path.join(table_dir, f"{meta.name}.heap")
        if os.path.exists(heap_path):
            os.remove(heap_path)

        data_cols = meta.data_columns()
        heap = _RealHeapFile(heap_path, data_cols)
        self._storage[meta.name] = _RealHeapAdapter(heap)
        meta.backend = "real"

        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = _csv.reader(f)
            next(reader, None)  # header
            for row in reader:
                if not row:
                    continue
                values = self._parse_csv_row(row, data_cols)
                if values is not None:
                    heap.add(values)

        for c in meta.columnas:
            indice = c.get("indice")
            if indice not in ("BTREE", "HASH", "RTREE", "SEQUENTIAL"):
                continue
            tipo = c["tipo"]
            idx_name = f"{meta.name}_{c['nombre']}"

            if indice == "SEQUENTIAL":
                if tipo not in ("INT", "VARCHAR", "STR"):
                    continue
                idx_path = os.path.join(table_dir, f"idx_{c['nombre']}.seqidx")
                for p in (idx_path, idx_path + ".aux"):
                    if os.path.exists(p):
                        os.remove(p)
                key_type = "INT" if tipo == "INT" else "STR"
                key_size = _AIRBNB_KEY_SIZE.get(c["nombre"], 50)
                seq = _RealSequentialIndex(
                    meta.name, idx_name, key_type=key_type,
                    key_size=key_size, filepath=idx_path,
                )
                meta.indices[c["nombre"]] = IndexInfo(
                    tipo="SEQUENTIAL",
                    instancia=_RealSequentialIndexAdapter(seq),
                )

            elif indice == "BTREE":
                if tipo not in ("INT", "VARCHAR", "STR"):
                    continue
                idx_path = os.path.join(table_dir, f"idx_{c['nombre']}.bin")
                if os.path.exists(idx_path):
                    os.remove(idx_path)
                if tipo == "INT":
                    tree = _RealBPlusTree(meta.name, idx_name, "INT", 0,
                                          filepath=idx_path)
                else:
                    key_size = _AIRBNB_KEY_SIZE.get(c["nombre"], 50)
                    tree = _RealBPlusTree(meta.name, idx_name, "STR", key_size,
                                          filepath=idx_path)
                meta.indices[c["nombre"]] = IndexInfo(
                    tipo="BTREE",
                    instancia=_RealBPlusTreeAdapter(tree),
                )

            elif indice == "HASH":

                hash_data = os.path.join(_PROJECT_ROOT, "data", f"{idx_name}.dat")
                hash_dir  = os.path.join(_PROJECT_ROOT, "data", f"{idx_name}_dir.dat")
                for p in (hash_data, hash_dir):
                    if os.path.exists(p):
                        os.remove(p)
                key_type = "INT" if tipo == "INT" else ("FLOAT" if tipo == "FLOAT" else "STR")
                key_size = _AIRBNB_KEY_SIZE.get(c["nombre"], 50)
                h = _RealHash(meta.name, idx_name, key_type, key_size=key_size)
                meta.indices[c["nombre"]] = IndexInfo(
                    tipo="HASH",
                    instancia=_RealHashAdapter(h),
                )

            elif indice == "RTREE":
                rtree_cols = c.get("rtree_cols") or []
                if len(rtree_cols) != 2:
                    continue
                rtree_path = os.path.join(_BASE, "data", f"{meta.name}_rtree.idx")
                if os.path.exists(rtree_path):
                    os.remove(rtree_path)
                rt = _RealRTree(meta.name)
                meta.indices[c["nombre"]] = IndexInfo(
                    tipo="RTREE",
                    rtree_cols=rtree_cols,
                    instancia=_RealRTreeAdapter(rt),
                )

        for pid, sid, values in self._storage[meta.name].iter_records():
            for col_name, info in meta.indices.items():
                if info.tipo == "RTREE":
                    cols = info.rtree_cols or []
                    if len(cols) != 2:
                        continue
                    i_x, i_y = meta.data_col_index(cols[0]), meta.data_col_index(cols[1])
                    if i_x < 0 or i_y < 0:
                        continue
                    info.instancia.add((values[i_x], values[i_y]), pid, sid)
                else:
                    i = meta.data_col_index(col_name)
                    if i < 0:
                        continue
                    info.instancia.add(values[i], pid, sid)

    @staticmethod
    def _parse_csv_row(row, columnas):
        if len(row) < len(columnas):
            return None
        out = []
        for cell, c in zip(row, columnas):
            tipo = (c.get("tipo") or "").upper()
            try:
                if tipo == "INT":
                    out.append(int(cell))
                elif tipo == "FLOAT":
                    out.append(float(cell))
                else:
                    out.append(cell)
            except (ValueError, TypeError):
                return None
        return out

    @staticmethod
    def _metrics_of(obj):
        """(disk_accesses, time_ms) de un adapter/instancia. 0 si no expone."""
        return (
            int(getattr(obj, "last_disk_accesses", 0) or 0),
            float(getattr(obj, "last_time_ms", 0.0) or 0.0),
        )

    def _insert(self, stmt):
        meta = self.catalog.get(stmt["tabla"])
        if meta is None:
            return {"status": "error", "msg": f"tabla inexistente: {stmt['tabla']}"}
        valores = stmt["valores_a_insertar"]
        data_cols = meta.data_columns()
        if len(valores) != len(data_cols):
            return {"status": "error",
                    "msg": f"aridad de VALUES no coincide: esperaba {len(data_cols)}, llegaron {len(valores)}"}

        t0 = time.time()
        storage = self._storage[meta.name]
        pid, sid = storage.add(valores)
        disk = self._metrics_of(storage)[0]

        for col_name, info in meta.indices.items():
            if info.instancia is None:
                continue
            if info.tipo == "RTREE":
                cols = info.rtree_cols or []
                if len(cols) != 2:
                    continue
                i_x, i_y = meta.data_col_index(cols[0]), meta.data_col_index(cols[1])
                if i_x >= 0 and i_y >= 0:
                    info.instancia.add((valores[i_x], valores[i_y]), pid, sid)
                    disk += self._metrics_of(info.instancia)[0]
            else:
                i = meta.data_col_index(col_name)
                if i >= 0:
                    info.instancia.add(valores[i], pid, sid)
                    disk += self._metrics_of(info.instancia)[0]

        return {"status": "ok", "op": "INSERT", "tabla": meta.name,
                "rid": [pid, sid], "valores": valores,
                "disk_accesses": disk,
                "execution_time_ms": round((time.time() - t0) * 1000, 3)}

    def _select_point(self, stmt):
        meta = self.catalog.get(stmt["tabla"])
        if meta is None:
            return {"status": "error", "msg": f"tabla inexistente: {stmt['tabla']}"}
        cond = stmt["condicion"]
        col, valor = cond["columna_where"], cond["valor"]
        info = meta.indices.get(col)
        if info is None:
            return {"status": "error", "msg": f"sin índice sobre {col}"}

        t0 = time.time()
        if hasattr(info.instancia, "search_all"):
            rids = info.instancia.search_all(valor)
        else:
            rid = info.instancia.search(valor)
            rids = [rid] if rid else []
        disk = self._metrics_of(info.instancia)[0]

        storage = self._storage[meta.name]
        filas = []
        for rid in rids:
            r = storage.get(*rid)
            disk += self._metrics_of(storage)[0]
            if r is not None:
                filas.append(r)

        return {"status": "ok", "op": "SELECT_POINT", "tabla": meta.name,
                "filas": filas, "n": len(filas),
                "disk_accesses": disk,
                "execution_time_ms": round((time.time() - t0) * 1000, 3),
                "indice_tipo": info.tipo}

    def _select_range(self, stmt):
        meta = self.catalog.get(stmt["tabla"])
        if meta is None:
            return {"status": "error", "msg": f"tabla inexistente: {stmt['tabla']}"}
        cond = stmt["condicion"]
        col, lo, hi = cond["columna_where"], cond["valor_inicio"], cond["valor_fin"]
        info = meta.indices.get(col)
        if info is None:
            return {"status": "error", "msg": f"sin índice sobre {col}"}

        t0 = time.time()
        rids = info.instancia.range_search(lo, hi)
        disk = self._metrics_of(info.instancia)[0]

        storage = self._storage[meta.name]
        filas = []
        for rid in rids:
            r = storage.get(*rid)
            disk += self._metrics_of(storage)[0]
            if r is not None:
                filas.append(r)

        return {"status": "ok", "op": "SELECT_RANGE", "tabla": meta.name,
                "filas": filas, "n": len(filas),
                "disk_accesses": disk,
                "execution_time_ms": round((time.time() - t0) * 1000, 3),
                "indice_tipo": info.tipo}

    def _select_spatial(self, stmt):
        meta = self.catalog.get(stmt["tabla"])
        if meta is None:
            return {"status": "error", "msg": f"tabla inexistente: {stmt['tabla']}"}
        cond = stmt["condicion"]
        col = cond["columna_where"]
        info = meta.indices.get(col)
        if info is None or info.tipo != "RTREE":
            return {"status": "error",
                    "msg": f"sin índice RTREE sobre {col}"}

        point = (cond["coordenada_x"], cond["coordenada_y"])
        op = stmt["tipo_operacion"]
        radio = cond.get("radio")
        k = cond.get("k_vecinos")

        t0 = time.time()
        if op == "SELECT_SPATIAL_RADIUS":
            rids = info.instancia.range_search_spatial(point, radio)
        elif op == "SELECT_SPATIAL_KNN":
            rids = info.instancia.knn(point, k)
        else:
            return {"status": "error", "msg": f"op espacial desconocida: {op}"}
        disk = self._metrics_of(info.instancia)[0]

        rcols = info.rtree_cols or []
        i_x = meta.data_col_index(rcols[0]) if len(rcols) >= 1 else -1
        i_y = meta.data_col_index(rcols[1]) if len(rcols) >= 2 else -1

        storage = self._storage[meta.name]
        filas = []
        puntos = []
        for rid in rids:
            r = storage.get(*rid)
            disk += self._metrics_of(storage)[0]
            if r is None:
                continue
            filas.append(r)
            if 0 <= i_x < len(r) and 0 <= i_y < len(r):
                puntos.append([r[i_x], r[i_y]])

        return {"status": "ok", "op": op, "tabla": meta.name,
                "filas": filas, "n": len(filas),
                "punto": list(point),
                "puntos_resultado": puntos,
                "rtree_cols": rcols,
                "radio": radio,
                "k": k,
                "disk_accesses": disk,
                "execution_time_ms": round((time.time() - t0) * 1000, 3),
                "indice_tipo": info.tipo}

    def _delete(self, stmt):
        meta = self.catalog.get(stmt["tabla"])
        if meta is None:
            return {"status": "error", "msg": f"tabla inexistente: {stmt['tabla']}"}
        cond = stmt["condicion"]
        col, valor = cond["columna_where"], cond["valor"]
        info = meta.indices.get(col)
        if info is None:
            return {"status": "error", "msg": f"sin índice sobre {col}"}

        t0 = time.time()
        rid = info.instancia.search(valor)
        disk = self._metrics_of(info.instancia)[0]
        if rid is None:
            return {"status": "ok", "op": "DELETE", "tabla": meta.name,
                    "borrados": 0,
                    "disk_accesses": disk,
                    "execution_time_ms": round((time.time() - t0) * 1000, 3)}

        storage = self._storage[meta.name]
        ok = storage.remove(*rid)
        disk += self._metrics_of(storage)[0]
        info.instancia.remove(valor)
        disk += self._metrics_of(info.instancia)[0]

        return {"status": "ok", "op": "DELETE", "tabla": meta.name,
                "rid": list(rid), "borrados": int(ok),
                "disk_accesses": disk,
                "execution_time_ms": round((time.time() - t0) * 1000, 3),
                "indice_tipo": info.tipo}

if __name__ == "__main__":
    print(f"[backend real disponible: {_REAL_BACKEND}]")
    ex = Executor()
    create = {
        "tipo_operacion": "CREATE_TABLE",
        "tabla": "airbnb_demo",
        "csv_path": "airbnb_1000.csv",
        "columnas": [
            {"nombre": "id",        "tipo": "INT",     "indice": "BTREE", "rtree_cols": None},
            {"nombre": "name",      "tipo": "VARCHAR", "indice": None,    "rtree_cols": None},
            {"nombre": "city",      "tipo": "VARCHAR", "indice": "BTREE", "rtree_cols": None},
            {"nombre": "lat",       "tipo": "FLOAT",   "indice": None,    "rtree_cols": None},
            {"nombre": "long",      "tipo": "FLOAT",   "indice": None,    "rtree_cols": None},
            {"nombre": "price",     "tipo": "FLOAT",   "indice": None,    "rtree_cols": None},
            {"nombre": "room_type", "tipo": "VARCHAR", "indice": None,    "rtree_cols": None},
            {"nombre": "cap",       "tipo": "INT",     "indice": None,    "rtree_cols": None},
        ],
    }
    print(ex.execute(create))
    print(ex.execute({"tipo_operacion": "SELECT_POINT", "tabla": "airbnb_demo",
                      "condicion": {"columna_where": "id", "operador": "=", "valor": 2577}}))
