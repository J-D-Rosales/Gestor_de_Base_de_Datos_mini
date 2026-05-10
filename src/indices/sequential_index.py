import os
import struct
import sys
import time
from pathlib import Path

_project_root = str(Path(__file__).resolve().parents[2])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.indices.base_index import BaseIndex
from src.buffer_manager import BufferManager


PAGE_SIZE = 4096
HEADER_FORMAT = "<i"
HEADER_SIZE = 4

NO_NEXT = -1


class SequentialIndex(BaseIndex):

    def __init__(self, table_name, idx_name, key_type="INT", key_size=50,
                 filepath=None, rebuild_threshold=None):
        super().__init__(table_name)
        self.idx_name = idx_name
        self.key_type = (key_type or "INT").upper()
        self.key_size = int(key_size) if self.key_type == "STR" else 0

        if self.key_type == "INT":
            self._key_fmt = "i"
        elif self.key_type == "STR":
            self._key_fmt = f"{self.key_size}s"
        else:
            raise ValueError(f"key_type no soportado: {key_type}")

        self.entry_format = "=?" + self._key_fmt + "iii"
        self.entry_size = struct.calcsize(self.entry_format)
        self.entries_per_page = (PAGE_SIZE - HEADER_SIZE) // self.entry_size
        if self.entries_per_page <= 0:
            raise ValueError(f"entry_size {self.entry_size} > page_size {PAGE_SIZE}")

        if filepath is None:
            filepath = os.path.join("src", "data", f"{idx_name}.seqidx")
        self.main_path = filepath
        self.aux_path = filepath + ".aux"

        self.main = BufferManager(self.main_path, PAGE_SIZE)
        self.aux = BufferManager(self.aux_path, PAGE_SIZE)

    def _coerce_key(self, key):
        if self.key_type == "INT":
            return int(key)
        if isinstance(key, str):
            key = key.encode("utf-8")
        return (key or b"").ljust(self.key_size, b"\x00")[:self.key_size]

    def _decode_key(self, raw):
        if self.key_type == "INT":
            return raw
        return raw.rstrip(b"\x00").decode("utf-8", errors="replace")

    def _pack_entry(self, key, pid, sid, nxt=NO_NEXT, is_deleted=False):
        return struct.pack(self.entry_format, is_deleted,
                           self._coerce_key(key), int(pid), int(sid), int(nxt))

    def _unpack_entry(self, raw):
        deleted, key_raw, pid, sid, nxt = struct.unpack(self.entry_format, raw)
        return deleted, self._decode_key(key_raw), pid, sid, nxt

    def _file_count(self, buf):
        n_pages = buf.num_pages()
        total = 0
        for pid in range(n_pages):
            raw = buf.read_page(pid)
            total += struct.unpack_from(HEADER_FORMAT, raw, 0)[0]
        return total

    def _read_entry(self, buf, idx):
        page_id = idx // self.entries_per_page
        slot = idx % self.entries_per_page
        if page_id < 0 or page_id >= buf.num_pages():
            return None
        raw = buf.read_page(page_id)
        count = struct.unpack_from(HEADER_FORMAT, raw, 0)[0]
        if slot >= count:
            return None
        offset = HEADER_SIZE + slot * self.entry_size
        return self._unpack_entry(raw[offset:offset + self.entry_size])

    def _write_entry(self, buf, idx, key, pid, sid, nxt=NO_NEXT, is_deleted=False):
        page_id = idx // self.entries_per_page
        slot = idx % self.entries_per_page
        if page_id < buf.num_pages():
            page = bytearray(buf.read_page(page_id))
        else:
            page = bytearray(PAGE_SIZE)
        offset = HEADER_SIZE + slot * self.entry_size
        page[offset:offset + self.entry_size] = self._pack_entry(
            key, pid, sid, nxt, is_deleted)
        new_count = max(struct.unpack_from(HEADER_FORMAT, page, 0)[0], slot + 1)
        struct.pack_into(HEADER_FORMAT, page, 0, new_count)
        buf.write_page(page_id, bytes(page))

    def _binary_search(self, key, n):
        coerced = self._coerce_key(key) if self.key_type == "STR" else int(key)
        lo, hi = 0, n - 1
        result = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            entry = self._read_entry(self.main, mid)
            if entry is None:
                hi = mid - 1
                continue
            _, ekey, _, _, _ = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if ekey_cmp <= coerced:
                result = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return result

    def add(self, key, page_id_value, slot_id_value):
        start = time.time()
        self.main.reset_io_cost()
        self.aux.reset_io_cost()

        n_main = self._file_count(self.main)
        n_aux = self._file_count(self.aux)
        
        # Umbral dinámico: 10% del archivo principal, mínimo 1000 registros
        dynamic_threshold = max(1000, n_main // 10)
        
        self._write_entry(self.aux, n_aux, key, page_id_value, slot_id_value)

        # Reconstruir si el archivo auxiliar excede el umbral dinámico
        if (n_aux + 1) >= dynamic_threshold:
            self._rebuild()

        return self._format_result(
            ["inserted"],
            self.main.get_io_cost() + self.aux.get_io_cost(),
            round((time.time() - start) * 1000, 3),
        )

    def search(self, key):
        start = time.time()
        self.main.reset_io_cost()
        self.aux.reset_io_cost()

        results = []
        coerced_target = self._coerce_key(key) if self.key_type == "STR" else int(key)

        n_main = self._file_count(self.main)
        base_idx = self._binary_search(key, n_main)

        # Expansión hacia atrás
        idx = base_idx
        while idx >= 0:
            entry = self._read_entry(self.main, idx)
            if entry is None:
                break
            deleted, ekey, pid, sid, _ = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if ekey_cmp != coerced_target:
                break
            if not deleted:
                results.append((pid, sid))
            idx -= 1

        # Expansión hacia adelante
        idx = base_idx + 1
        while idx < n_main:
            entry = self._read_entry(self.main, idx)
            if entry is None:
                break
            deleted, ekey, pid, sid, _ = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if ekey_cmp != coerced_target:
                break
            if not deleted:
                results.append((pid, sid))
            idx += 1

        n_aux = self._file_count(self.aux)
        for i in range(n_aux):
            entry = self._read_entry(self.aux, i)
            if entry is None:
                continue
            deleted, ekey, pid, sid, _ = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if not deleted and ekey_cmp == coerced_target:
                results.append((pid, sid))

        return self._format_result(
            results,
            self.main.get_io_cost() + self.aux.get_io_cost(),
            round((time.time() - start) * 1000, 3),
        )

    def range_search(self, begin_key, end_key):
        start = time.time()
        self.main.reset_io_cost()
        self.aux.reset_io_cost()

        lo = self._coerce_key(begin_key) if self.key_type == "STR" else int(begin_key)
        hi = self._coerce_key(end_key) if self.key_type == "STR" else int(end_key)

        results = []

        n_main = self._file_count(self.main)
        start_idx = max(0, self._binary_search(begin_key, n_main))

        for idx in range(start_idx, n_main):
            entry = self._read_entry(self.main, idx)
            if entry is None:
                continue
            deleted, ekey, pid, sid, _ = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if ekey_cmp > hi:
                break
            if not deleted and lo <= ekey_cmp <= hi:
                results.append((pid, sid))

        n_aux = self._file_count(self.aux)
        for i in range(n_aux):
            entry = self._read_entry(self.aux, i)
            if entry is None:
                continue
            deleted, ekey, pid, sid, _ = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if not deleted and lo <= ekey_cmp <= hi:
                results.append((pid, sid))

        return self._format_result(
            results,
            self.main.get_io_cost() + self.aux.get_io_cost(),
            round((time.time() - start) * 1000, 3),
        )

    def remove(self, key):
        start = time.time()
        self.main.reset_io_cost()
        self.aux.reset_io_cost()

        coerced = self._coerce_key(key) if self.key_type == "STR" else int(key)
        removed = 0

        n_main = self._file_count(self.main)
        base_idx = self._binary_search(key, n_main)
        
        # Expansión hacia atrás
        idx = base_idx
        while idx >= 0:
            entry = self._read_entry(self.main, idx)
            if entry is None:
                break
            deleted, ekey, pid, sid, nxt = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if ekey_cmp != coerced:
                break
            # Soft Delete
            if not deleted:
                self._write_entry(self.main, idx, ekey, pid, sid, nxt, True)
                removed += 1
            idx -= 1

        # Expansión hacia adelante
        idx = base_idx + 1
        while idx < n_main:
            entry = self._read_entry(self.main, idx)
            if entry is None:
                break
            deleted, ekey, pid, sid, nxt = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            
            if ekey_cmp != coerced:
                break
            # Soft Delete
            if not deleted:
                self._write_entry(self.main, idx, ekey, pid, sid, nxt, True)
                removed += 1
            idx += 1

        # Escaneo secuencial en archivo auxiliar
        n_aux = self._file_count(self.aux)
        for i in range(n_aux):
            entry = self._read_entry(self.aux, i)
            if entry is None:
                continue
            deleted, ekey, pid, sid, nxt = entry
            ekey_cmp = self._coerce_key(ekey) if self.key_type == "STR" else int(ekey)
            if not deleted and ekey_cmp == coerced:
                self._write_entry(self.aux, i, ekey, pid, sid, nxt, True)
                removed += 1

        return self._format_result(
            ["removed"] if removed else [],
            self.main.get_io_cost() + self.aux.get_io_cost(),
            round((time.time() - start) * 1000, 3),
        )

    def _read_all_entries(self, buf):
        out = []
        n_pages = buf.num_pages()
        for page_id in range(n_pages):
            raw = buf.read_page(page_id)
            count = struct.unpack_from(HEADER_FORMAT, raw, 0)[0]
            for slot in range(count):
                offset = HEADER_SIZE + slot * self.entry_size
                deleted, key, pid, sid, _ = self._unpack_entry(
                    raw[offset:offset + self.entry_size])
                if not deleted:
                    out.append((key, pid, sid))
        return out

    def _truncate(self, buf, path):
        try:
            buf.flush()
        except Exception:
            pass
        try:
            buf._fh.close()
        except Exception:
            pass
        with open(path, "wb"):
            pass
        buf._cache.clear()
        buf._dirty.clear()
        buf._max_written_page = -1
        buf._fh = open(path, "r+b")

    def _rebuild(self):

        # Cargar y ordenar solo el archivo auxiliar en RAM
        aux_entries = self._read_all_entries(self.aux)
        if self.key_type == "STR":
            aux_entries.sort(key=lambda e: self._coerce_key(e[0]))
        else:
            aux_entries.sort(key=lambda e: int(e[0]))

        # Crear un BufferManager temporal para el nuevo archivo principal
        temp_path = self.main_path + ".tmp"
        temp_buf = BufferManager(temp_path, PAGE_SIZE)
        
        n_main = self._file_count(self.main)
        main_idx = 0
        aux_idx = 0
        new_main_idx = 0

        # Proceso de Fusión (Merge) entre el archivo principal y el auxiliar ordenado
        while main_idx < n_main or aux_idx < len(aux_entries):
            
            # Buscar el siguiente registro activo en el archivo principal
            main_entry = None
            while main_idx < n_main:
                entry = self._read_entry(self.main, main_idx)
                if entry and not entry[0]:  # Si existe y no está eliminado
                    main_entry = entry
                    break
                main_idx += 1

            # Comparar e insertar 
            # Caso A: Se acabó el principal, vaciamos de golpe lo que queda del auxiliar
            if main_entry is None:
                while aux_idx < len(aux_entries):
                    k, p, s = aux_entries[aux_idx]
                    self._write_entry(temp_buf, new_main_idx, k, p, s)
                    new_main_idx += 1
                    aux_idx += 1
                break

            # Caso B: Se acabó el auxiliar, escribimos el principal actual y continuamos
            if aux_idx >= len(aux_entries):
                _, m_key, m_p, m_s, _ = main_entry
                self._write_entry(temp_buf, new_main_idx, m_key, m_p, m_s)
                new_main_idx += 1
                main_idx += 1
                continue

            # Caso C: Ambos tienen datos, comparamos claves para ver quién "gana"
            _, m_key, m_p, m_s, _ = main_entry
            m_key_cmp = self._coerce_key(m_key) if self.key_type == "STR" else int(m_key)
            a_key_cmp = self._coerce_key(aux_entries[aux_idx][0]) if self.key_type == "STR" else int(aux_entries[aux_idx][0])

            if m_key_cmp <= a_key_cmp:
                self._write_entry(temp_buf, new_main_idx, m_key, m_p, m_s)
                main_idx += 1
            else:
                k, p, s = aux_entries[aux_idx]
                self._write_entry(temp_buf, new_main_idx, k, p, s)
                aux_idx += 1
            
            new_main_idx += 1

        # Guardar cambios y cerrar los BufferManagers
        temp_buf.close()
        self.main.close()

        # Reemplazo a nivel de Sistema Operativo
        os.replace(temp_path, self.main_path)
        
        # Reiniciar el main instanciando un nuevo BufferManager limpio
        self.main = BufferManager(self.main_path, PAGE_SIZE)
        
        # Vaciar el archivo auxiliar para el siguiente ciclo
        self._truncate(self.aux, self.aux_path)

    def flush(self):
        self.main.flush()
        self.aux.flush()
