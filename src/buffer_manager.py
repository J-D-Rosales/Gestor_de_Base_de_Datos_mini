# src/buffer_manager.py
import os
from collections import OrderedDict


class BufferManager:
    """
    Buffer de páginas con caché LRU y política write-back.

    - Mantiene un único file handle abierto (evita open/close por página).
    - read_page: si la página está en caché es un hit (no cuenta como I/O);
      si no, se lee de disco, se inserta en la caché y, si esta está llena,
      se desaloja la página menos usada (escribiéndola a disco si está dirty).
    - write_page: actualiza la caché y marca la página como dirty. Solo
      llega a disco al ser desalojada o cuando se llama a flush()/close().
    - get_io_cost cuenta solo I/O real a disco; los hits de caché se exponen
      por separado en cache_hits/cache_misses.
    """

    DEFAULT_CAPACITY = 1024  # 1024 * 4KB = 4 MB por índice

    def __init__(self, filepath, page_size=4096, capacity=DEFAULT_CAPACITY):
        self.filepath = filepath
        self.page_size = page_size
        self.capacity = max(1, int(capacity))

        # I/O real a disco
        self.reads = 0
        self.writes = 0

        # Estadísticas del caché
        self.cache_hits = 0
        self.cache_misses = 0

        # LRU: page_id -> bytes. La más recientemente usada queda al final.
        self._cache: "OrderedDict[int, bytes]" = OrderedDict()
        # Páginas en caché con cambios pendientes de escribir a disco.
        self._dirty: set[int] = set()

        # Crea el archivo si no existe y abre un handle persistente en r+b.
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'wb'):
                pass
        self._fh = open(self.filepath, 'r+b')

    # ── I/O directo a disco (sin tocar la caché) ──────────────────────────
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
        # Desaloja LRU hasta que quepa una nueva entrada.
        while len(self._cache) >= self.capacity:
            evict_id, evict_data = self._cache.popitem(last=False)
            if evict_id in self._dirty:
                self._disk_write(evict_id, evict_data)
                self._dirty.discard(evict_id)

    # ── API pública (idéntica al BufferManager original) ──────────────────
    def read_page(self, page_id: int) -> bytes:
        """Lee exactamente una página (4KB), sirviendo desde la caché si existe."""
        cached = self._cache.get(page_id)
        if cached is not None:
            self.cache_hits += 1
            self._cache.move_to_end(page_id)  # marcar como recientemente usada
            return cached

        self.cache_misses += 1
        data = self._disk_read(page_id)
        self._evict_if_needed()
        self._cache[page_id] = data
        return data

    def write_page(self, page_id: int, data: bytes):
        """Escribe en la caché y marca la página como dirty (write-back)."""
        if len(data) > self.page_size:
            raise ValueError(
                f"CRÍTICO: La data ({len(data)} bytes) excede el tamaño de página de {self.page_size} bytes."
            )

        # Snapshot inmutable: bytes() copia bytearray para que mutaciones
        # posteriores del caller no corrompan la entrada en caché.
        padded = bytes(data).ljust(self.page_size, b'\0')

        if page_id in self._cache:
            self._cache[page_id] = padded
            self._cache.move_to_end(page_id)
        else:
            self._evict_if_needed()
            self._cache[page_id] = padded

        self._dirty.add(page_id)

    def flush(self):
        """Vuelca a disco todas las páginas dirty y sincroniza."""
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
        # Best-effort: intenta volcar y cerrar al recolectar el objeto.
        try:
            self.close()
        except Exception:
            pass

    def get_io_cost(self) -> int:
        """Total de accesos a disco reales (lecturas + escrituras), sin contar hits de caché."""
        return self.reads + self.writes

    def reset_io_cost(self):
        """Reinicia los contadores antes de cada nueva consulta SQL.
        No invalida la caché ni descarta páginas dirty."""
        self.reads = 0
        self.writes = 0
        self.cache_hits = 0
        self.cache_misses = 0
