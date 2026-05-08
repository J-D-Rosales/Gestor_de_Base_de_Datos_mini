# src/buffer_manager.py
import os
from collections import OrderedDict


class BufferManager:

    DEFAULT_CAPACITY = 64

    def __init__(self, filepath, page_size=4096, capacity=DEFAULT_CAPACITY):
        self.filepath = filepath
        self.page_size = page_size
        self.capacity = max(1, int(capacity))

        self.reads = 0
        self.writes = 0

        self.cache_hits = 0
        self.cache_misses = 0

        self.write_calls = 0

        self._cache: "OrderedDict[int, bytes]" = OrderedDict()
        self._dirty: set[int] = set()
        self._max_written_page = -1

        if not os.path.exists(self.filepath):
            with open(self.filepath, 'wb'):
                pass
        self._fh = open(self.filepath, 'r+b')

    def _disk_read(self, page_id: int) -> bytes:
        self.reads += 1
        self._fh.seek(page_id * self.page_size)
        data = self._fh.read(self.page_size)
        return data.ljust(self.page_size, b'\0')

    def _disk_write(self, page_id: int, data: bytes):
        self.writes += 1
        padded = data.ljust(self.page_size, b'\0')
        self._fh.seek(page_id * self.page_size)
        self._fh.write(padded)

    def _evict_if_needed(self):
        while len(self._cache) >= self.capacity:
            evict_id, evict_data = self._cache.popitem(last=False)
            if evict_id in self._dirty:
                self._disk_write(evict_id, evict_data)
                self._dirty.discard(evict_id)

    def read_page(self, page_id: int) -> bytes:
        """Lee exactamente una página (4KB), sirviendo desde la caché si existe."""
        cached = self._cache.get(page_id)
        if cached is not None:
            self.cache_hits += 1
            self._cache.move_to_end(page_id)
            return cached

        self.cache_misses += 1
        data = self._disk_read(page_id)
        self._evict_if_needed()
        self._cache[page_id] = data
        return data

    def write_page(self, page_id: int, data: bytes):
        if len(data) > self.page_size:
            raise ValueError(
                f"CRÍTICO: La data ({len(data)} bytes) excede el tamaño de página de {self.page_size} bytes."
            )

        self.write_calls += 1

        padded = bytes(data).ljust(self.page_size, b'\0')

        if page_id in self._cache:
            self._cache[page_id] = padded
            self._cache.move_to_end(page_id)
        else:
            self._evict_if_needed()
            self._cache[page_id] = padded

        self._dirty.add(page_id)
        if page_id > self._max_written_page:
            self._max_written_page = page_id

    def flush(self):
        for page_id in list(self._dirty):
            self._disk_write(page_id, self._cache[page_id])
        self._dirty.clear()
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
        except (OSError, ValueError):
            pass

    def close(self):
        try:
            self.flush()
        finally:
            try:
                self._fh.close()
            except Exception:
                pass

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def num_pages(self) -> int:
        try:
            disk_size = os.path.getsize(self.filepath)
        except OSError:
            disk_size = 0
        disk_pages = (disk_size + self.page_size - 1) // self.page_size
        cache_pages = self._max_written_page + 1
        return max(disk_pages, cache_pages)

    def get_io_cost(self) -> int:
        return self.cache_hits + self.cache_misses + self.write_calls

    def reset_io_cost(self):
        self.reads = 0
        self.writes = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.write_calls = 0
