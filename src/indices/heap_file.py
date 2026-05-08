import struct

from buffer_manager import BufferManager


class HeapFile:
    HEADER_FORMAT = "<i"   # count: int32 LE
    HEADER_SIZE = 4
    DEFAULT_PAGE_SIZE = 4096

    # Mismo mapeo que _TYPE_TO_FORMAT del executor (FLOAT como 'd' = double).
    _TYPE_TO_FMT = {
        "INT": "i",
        "FLOAT": "d",
        "VARCHAR": "50s",
        "STR": "50s",
    }
    _STR_KINDS = {"VARCHAR", "STR"}
    _STR_LEN = 50

    def __init__(self, filepath, columnas, page_size=DEFAULT_PAGE_SIZE):
        self.filepath = filepath
        self.page_size = page_size
        self.columnas = columnas

        # body_format: solo el cuerpo, sin is_deleted.
        body_codes = "".join(self._TYPE_TO_FMT.get((c.get("tipo") or "").upper(), "50s")
                             for c in columnas)
        self.body_format = "=" + body_codes
        self.body_size = struct.calcsize(self.body_format)
        # record_format: ?(is_deleted) + cuerpo.
        self.record_format = "=?" + body_codes
        self.record_size = struct.calcsize(self.record_format)

        self.records_per_page = (page_size - self.HEADER_SIZE) // self.record_size
        if self.records_per_page <= 0:
            raise ValueError(f"record_size {self.record_size} > page_size {page_size}")

        self.buffer = BufferManager(filepath, page_size)
        # Posición del próximo RID a emitir.
        self._next_page = 0
        self._next_slot = 0
        self._infer_next_position()

    def _infer_next_position(self):
        n = self.buffer.num_pages()
        if n == 0:
            return
        last = n - 1
        raw = self.buffer.read_page(last)
        count = struct.unpack_from(self.HEADER_FORMAT, raw, 0)[0]
        if count >= self.records_per_page:
            self._next_page, self._next_slot = n, 0
        else:
            self._next_page, self._next_slot = last, count

    def _coerce_for_pack(self, values):
        out = []
        for v, c in zip(values, self.columnas):
            tipo = (c.get("tipo") or "").upper()
            if tipo in self._STR_KINDS:
                if isinstance(v, str):
                    v = v.encode("utf-8")
                v = (v or b"").ljust(self._STR_LEN, b"\x00")[:self._STR_LEN]
            elif tipo == "INT":
                v = int(v)
            elif tipo == "FLOAT":
                v = float(v)
            out.append(v)
        return out

    def _pack_record(self, values, is_deleted=False):
        coerced = self._coerce_for_pack(values)
        return struct.pack(self.record_format, is_deleted, *coerced)

    def _unpack_record(self, raw):
        unpacked = struct.unpack(self.record_format, raw)
        is_deleted = unpacked[0]
        if is_deleted:
            return None
        out = []
        for v, c in zip(unpacked[1:], self.columnas):
            tipo = (c.get("tipo") or "").upper()
            if tipo in self._STR_KINDS and isinstance(v, bytes):
                v = v.rstrip(b"\x00").decode("utf-8", errors="replace")
            out.append(v)
        return out

    def add(self, values):
        if len(values) != len(self.columnas):
            raise ValueError(f"esperaba {len(self.columnas)} valores, llegaron {len(values)}")

        page_id, slot_id = self._next_page, self._next_slot
        if slot_id == 0:
            page = bytearray(self.page_size)
        else:
            page = bytearray(self.buffer.read_page(page_id))
        offset = self.HEADER_SIZE + slot_id * self.record_size
        page[offset:offset + self.record_size] = self._pack_record(values)
        struct.pack_into(self.HEADER_FORMAT, page, 0, slot_id + 1)
        self.buffer.write_page(page_id, bytes(page))

        self._next_slot += 1
        if self._next_slot >= self.records_per_page:
            self._next_page += 1
            self._next_slot = 0
        return page_id, slot_id

    def get(self, page_id, slot_id):
        if page_id < 0 or page_id >= self.buffer.num_pages():
            return None
        raw = self.buffer.read_page(page_id)
        count = struct.unpack_from(self.HEADER_FORMAT, raw, 0)[0]
        if slot_id < 0 or slot_id >= count:
            return None
        offset = self.HEADER_SIZE + slot_id * self.record_size
        return self._unpack_record(raw[offset:offset + self.record_size])

    def remove(self, page_id, slot_id):
        if page_id < 0 or page_id >= self.buffer.num_pages():
            return False
        page = bytearray(self.buffer.read_page(page_id))
        count = struct.unpack_from(self.HEADER_FORMAT, page, 0)[0]
        if slot_id < 0 or slot_id >= count:
            return False
        offset = self.HEADER_SIZE + slot_id * self.record_size
        existing = struct.unpack(self.record_format, page[offset:offset + self.record_size])
        if existing[0]:
            return False
        body_values = list(existing[1:])
        body_decoded = []
        for v, c in zip(body_values, self.columnas):
            tipo = (c.get("tipo") or "").upper()
            if tipo in self._STR_KINDS and isinstance(v, bytes):
                body_decoded.append(v.rstrip(b"\x00").decode("utf-8", errors="replace"))
            else:
                body_decoded.append(v)
        page[offset:offset + self.record_size] = self._pack_record(body_decoded, is_deleted=True)
        self.buffer.write_page(page_id, bytes(page))
        return True

    def iter_records(self):
        n = self.buffer.num_pages()
        for pid in range(n):
            raw = self.buffer.read_page(pid)
            count = struct.unpack_from(self.HEADER_FORMAT, raw, 0)[0]
            for sid in range(count):
                offset = self.HEADER_SIZE + sid * self.record_size
                values = self._unpack_record(raw[offset:offset + self.record_size])
                if values is not None:
                    yield pid, sid, values

    def num_pages(self):
        return self.buffer.num_pages()

    def flush(self):
        self.buffer.flush()
