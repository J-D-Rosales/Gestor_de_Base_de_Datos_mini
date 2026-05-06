import json
import math
import os
import time

from .base_index import BaseIndex
from ..buffer_manager import BufferManager


class RTree(BaseIndex):
    def __init__(self, table_name):
        super().__init__(table_name)

        self.filename = f"src/data/{table_name}_rtree.idx"
        self.buffer = BufferManager(self.filename)

    def _result(self, data, start_time):
        return self._format_result(
            data,
            self.buffer.get_io_cost(),
            round((time.time() - start_time) * 1000, 3),
        )

    def _page_count(self):
        size = os.path.getsize(self.filename)
        if size == 0:
            return 0
        return (size + self.buffer.page_size - 1) // self.buffer.page_size

    def _page_to_records(self, page_id):
        raw = self.buffer.read_page(page_id)
        payload = raw.rstrip(b"\0")

        if not payload:
            return []

        return json.loads(payload.decode("utf-8"))

    def _write_records(self, page_id, records):
        encoded = json.dumps(records).encode("utf-8")
        self.buffer.write_page(page_id, encoded)

    def _distance(self, p1, p2):
        return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

    def _rid(self, item):
        return {
            "page_id": item["page_id"],
            "slot_id": item["slot_id"],
        }

    def add(self, key, page_id_value, slot_id_value):
        start = time.time()
        self.buffer.reset_io_cost()

        entry = {
            "point": [key[0], key[1]],
            "page_id": page_id_value,
            "slot_id": slot_id_value,
        }

        total_pages = self._page_count()

        for page_id in range(total_pages):
            records = self._page_to_records(page_id)
            candidate = records + [entry]

            if len(json.dumps(candidate).encode("utf-8")) <= self.buffer.page_size:
                self._write_records(page_id, candidate)
                return self._result(["inserted"], start)

        self._write_records(total_pages, [entry])
        return self._result(["inserted"], start)

    def search(self, key):
        start = time.time()
        self.buffer.reset_io_cost()

        results = []

        for page_id in range(self._page_count()):
            records = self._page_to_records(page_id)

            for item in records:
                if tuple(item["point"]) == tuple(key):
                    results.append(self._rid(item))

        return self._result(results, start)

    def remove(self, key):
        start = time.time()
        self.buffer.reset_io_cost()

        removed = False

        for page_id in range(self._page_count()):
            records = self._page_to_records(page_id)
            new_records = [r for r in records if tuple(r["point"]) != tuple(key)]

            if len(new_records) != len(records):
                self._write_records(page_id, new_records)
                removed = True

        return self._result(["removed"] if removed else [], start)

    def range_search(self, begin_key, end_key):
        start = time.time()
        self.buffer.reset_io_cost()

        x1, y1 = begin_key
        x2, y2 = end_key

        results = []

        for page_id in range(self._page_count()):
            records = self._page_to_records(page_id)

            for item in records:
                x, y = item["point"]

                if x1 <= x <= x2 and y1 <= y <= y2:
                    results.append(self._rid(item))

        return self._result(results, start)

    def range_search_spatial(self, point, radius):
        start = time.time()
        self.buffer.reset_io_cost()

        results = []

        for page_id in range(self._page_count()):
            records = self._page_to_records(page_id)

            for item in records:
                if self._distance(tuple(item["point"]), point) <= radius:
                    results.append(self._rid(item))

        return self._result(results, start)

    def knn(self, point, k):
        start = time.time()
        self.buffer.reset_io_cost()

        candidates = []

        for page_id in range(self._page_count()):
            records = self._page_to_records(page_id)

            for item in records:
                distance = self._distance(tuple(item["point"]), point)
                candidates.append((distance, self._rid(item)))

        candidates.sort(key=lambda x: x[0])

        return self._result([rid for _, rid in candidates[:k]], start)
