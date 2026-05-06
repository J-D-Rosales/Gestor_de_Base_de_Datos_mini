import struct
import os
import time
from typing import Optional
import csv

# ════════════════════════════════════
#  VARIABLES GLOBALES
# ════════════════════════════════════

# Registro
RECORD_FORMAT = '=i 60s 30s d d 20s i i i ?'
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

# Página
PAGE_SIZE = 1024 * 4    # 4KB
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE # 28 registros por página

# Cabecera (guardanlos primeros 8 bytes de la página 0)
HEADER_FORMAT = '=q'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)    # 8 bytes para el número de páginas
HEADER_PAGE_SIZE = 0    # Página 0 se reserva para la cabecera

# Define desde dónde comienzan los datos en el archivo
DATA_OFFSET = PAGE_SIZE  # Los datos comienzan en la página 1
NO_NEXT = -1  # No hay página siguiente

# ════════════════════════════════════
#  CLASE RECORD
# ════════════════════════════════════

class Record:
    __slots__ = ("listing_id", "name", "city", "latitude",
                 "longitude", "price", "room_type", "accommodates",
                 "next", "is_deleted")

    def __init__(self, listing_id: int, name: str, city: str,
                 latitude: float, longitude: float, price: int,
                 room_type: str, accommodates: int,
                 next: int = NO_NEXT, is_deleted: bool = False):
        self.listing_id   = listing_id
        self.name         = name[:60]
        self.city         = city[:30]
        self.latitude     = latitude
        self.longitude    = longitude
        self.price        = price
        self.room_type    = room_type[:20]
        self.accommodates = accommodates
        self.next         = next
        self.is_deleted   = is_deleted

    # Empaquetar el objeto Record a bytes
    def pack(self) -> bytes:
        return struct.pack(
            RECORD_FORMAT,
            self.listing_id,
            self.name.encode("utf-8").ljust(60, b'\x00')[:60],
            self.city.encode("utf-8").ljust(30, b'\x00')[:30],
            self.latitude,
            self.longitude,
            self.room_type.encode("utf-8").ljust(20, b'\x00')[:20],
            self.price,
            self.accommodates,
            self.next,
            self.is_deleted,
        )
 
    # Desempaquetar bytes a un objeto Record
    @staticmethod
    def unpack(data: bytes) -> "Record":
        (lid, name_b, city_b, lat, lon,
         room_b, price, acc, nxt, deleted) = struct.unpack(RECORD_FORMAT, data)
        return Record(
            listing_id   = lid,
            name         = name_b.rstrip(b'\x00').decode("utf-8", errors="replace"),
            city         = city_b.rstrip(b'\x00').decode("utf-8", errors="replace"),
            latitude     = lat,
            longitude    = lon,
            price        = price,
            room_type    = room_b.rstrip(b'\x00').decode("utf-8", errors="replace"),
            accommodates = acc,
            next         = nxt,
            is_deleted   = bool(deleted),
        )

    # Imprimir el registro de forma legible
    def __repr__(self):
        status = " [BORRADO]" if self.is_deleted else ""
        return (f"Record(id={self.listing_id}, name={self.name!r}, "
                f"city={self.city!r}, price={self.price}, "
                f"room={self.room_type!r}, "
                f"lat={self.latitude:.5f}, lon={self.longitude:.5f}, "
                f"acc={self.accommodates}, next={self.next}){status}")

# ════════════════════════════════════
#  CLASE SEQUENTIALFILE
# ════════════════════════════════════

class SequentialFile:
    def __init__(self, main_path: str, aux_path: str, K: int = 50):
        self.main_path = main_path
        self.aux_path  = aux_path
        self.K         = K
        self.disk_reads  = 0
        self.disk_writes = 0
 
        # Asegura que ambos archivos existan, si no, llama a _init_file para inicializarlos
        for path in (self.main_path, self.aux_path):
            if not os.path.exists(path):
                self._init_file(path)
 
    # Inicializa un nuevo archivo con una página de cabecera vacía
    def _init_file(self, path: str):
        """Crea el archivo con una pagina de cabecera (n_records = 0)."""
        with open(path, "wb") as f:
            page = bytearray(PAGE_SIZE)
            struct.pack_into(HEADER_FORMAT, page, 0, 0)
            f.write(bytes(page))
        
    # Métricas de acceso a disco
    def reset_counters(self):
        self.disk_reads  = 0
        self.disk_writes = 0
 
    def get_stats(self) -> dict:
        return {"disk_reads": self.disk_reads, "disk_writes": self.disk_writes}

    # Lectura y escritura de la cabecera (número de registros)
    def _read_count(self, f) -> int:
        f.seek(0)
        page = f.read(PAGE_SIZE)
        self.disk_reads += 1
        (n,) = struct.unpack_from(HEADER_FORMAT, page, 0)
        return n
 
    def _write_count(self, f, n: int):
        f.seek(0)
        page = bytearray(f.read(PAGE_SIZE))
        self.disk_reads += 1
        if len(page) < PAGE_SIZE:
            page += b'\x00' * (PAGE_SIZE - len(page))
        struct.pack_into(HEADER_FORMAT, page, 0, n)
        f.seek(0)
        f.write(bytes(page))
        self.disk_writes += 1

    # Lectura y escritura de registros por índice
    def _read_record(self, f, idx: int) -> Optional[Record]:
        page_no = 1 + (idx // RECORDS_PER_PAGE)
        in_page_off = (idx % RECORDS_PER_PAGE) * RECORD_SIZE

        f.seek(page_no * PAGE_SIZE)
        page = f.read(PAGE_SIZE)
        self.disk_reads += 1

        if len(page) < in_page_off + RECORD_SIZE:
            return None
        return Record.unpack(page[in_page_off : in_page_off + RECORD_SIZE])

    def _write_record(self, f, idx: int, rec: Record):
        page_no = 1 + (idx // RECORDS_PER_PAGE)
        in_page_off = (idx % RECORDS_PER_PAGE) * RECORD_SIZE

        f.seek(page_no * PAGE_SIZE)
        page = bytearray(f.read(PAGE_SIZE))
        self.disk_reads += 1

        if len(page) < PAGE_SIZE:
            page += b'\x00' * (PAGE_SIZE - len(page))

        page[in_page_off : in_page_off + RECORD_SIZE] = rec.pack()
        
        f.seek(page_no * PAGE_SIZE)
        f.write(bytes(page))
        self.disk_writes += 1
    
    def _binary_search(self, f, key: int, n: int) -> int:
        lo, hi = 0, n - 1
        result = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            rec = self._read_record(f, mid)
            if rec is None:
                break
            if rec.listing_id <= key:
                result = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return result

    def _follow_next(self, fm, fa, next_ptr: int) -> Optional[Record]:
        if next_ptr == NO_NEXT: return None
        if next_ptr >= 0: return self._read_record(fm, next_ptr)
        aux_idx = -(next_ptr + 2)
        return self._read_record(fa, aux_idx)

    def search(self, key: int) -> Optional[Record]:
        self.reset_counters()
        with open(self.main_path, "rb") as fm, open(self.aux_path, "rb") as fa:
            n_main = self._read_count(fm)
            idx = self._binary_search(fm, key, n_main)

            if idx >= 0:
                rec = self._read_record(fm, idx)
                while rec is not None:
                    if not rec.is_deleted and rec.listing_id == key:
                        return rec
                    if rec.next == NO_NEXT:
                        break
                    rec = self._follow_next(fm, fa, rec.next)

            n_aux = self._read_count(fa)
            for i in range(n_aux):
                rec = self._read_record(fa, i)
                if rec and not rec.is_deleted and rec.listing_id == key:
                    return rec
        return None

    def range_search(self, begin_key: int, end_key: int) -> list:
        self.reset_counters()
        t0 = time.perf_counter()
        results = []

        with open(self.main_path, "rb") as fm:
            n_main = self._read_count(fm)
            total_pages = 1 + ((n_main + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE)
            stop_search = False

            for page_no in range(1, total_pages):
                fm.seek(page_no * PAGE_SIZE)
                page_data = fm.read(PAGE_SIZE)
                self.disk_reads += 1

                for i in range(RECORDS_PER_PAGE):
                    idx = ((page_no - 1) * RECORDS_PER_PAGE) + i
                    if idx >= n_main: break

                    rec_data = page_data[i * RECORD_SIZE : (i + 1) * RECORD_SIZE]
                    rec = Record.unpack(rec_data)

                    if not rec.is_deleted:
                        if begin_key <= rec.listing_id <= end_key:
                            results.append(rec)
                        elif rec.listing_id > end_key:
                            stop_search = True
                            break
                if stop_search:
                    break

        # Búsqueda en el Auxiliar (sin ordenamiento, lectura secuencial)
        with open(self.aux_path, "rb") as fa:
            n_aux = self._read_count(fa)
            total_aux_pages = 1 + ((n_aux + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE)

            for page_no in range(1, total_aux_pages):
                fa.seek(page_no * PAGE_SIZE)
                page_data = fa.read(PAGE_SIZE)
                self.disk_reads += 1

                for i in range(RECORDS_PER_PAGE):
                    idx = ((page_no - 1) * RECORDS_PER_PAGE) + i
                    if idx >= n_aux: break

                    rec_data = page_data[i * RECORD_SIZE : (i + 1) * RECORD_SIZE]
                    rec = Record.unpack(rec_data)

                    if not rec.is_deleted and (begin_key <= rec.listing_id <= end_key):
                        results.append(rec)

        results.sort(key=lambda r: r.listing_id)
        
        print(f"[range_search] {len(results)} reg. | reads: {self.disk_reads} | {(time.perf_counter()-t0)*1000:.2f}ms")
        return results

    def add(self, rec: Record):
        self.reset_counters()
        with open(self.main_path, "r+b") as fm, open(self.aux_path, "r+b") as fa:
            n_main = self._read_count(fm)
            n_aux  = self._read_count(fa)
            idx = self._binary_search(fm, rec.listing_id, n_main)

            if idx >= 0:
                pred = self._read_record(fm, idx)
                rec.next = pred.next
                pred.next = -(n_aux + 2)
                self._write_record(fa, n_aux, rec)
                self._write_record(fm, idx, pred)
            else:
                rec.next = NO_NEXT
                self._write_record(fa, n_aux, rec)

            self._write_count(fa, n_aux + 1)

        if (n_aux + 1) >= self.K:
            self._rebuild()

    def remove(self, key: int) -> bool:
        self.reset_counters()
        deleted = False

        with open(self.main_path, "r+b") as fm:
            n_main = self._read_count(fm)
            idx = self._binary_search(fm, key, n_main)
            if idx >= 0:
                rec = self._read_record(fm, idx)
                if rec and not rec.is_deleted and rec.listing_id == key:
                    rec.is_deleted = True
                    self._write_record(fm, idx, rec)
                    deleted = True

        if not deleted:
            with open(self.aux_path, "r+b") as fa:
                n_aux = self._read_count(fa)
                for i in range(n_aux):
                    rec = self._read_record(fa, i)
                    if rec and not rec.is_deleted and rec.listing_id == key:
                        rec.is_deleted = True
                        self._write_record(fa, i, rec)
                        deleted = True
                        break
        return deleted


    def _rebuild(self):
        print("[rebuild] Iniciando reconstrucción física estructurada...")
        aux_recs = []
        
        # 1. Cargamos el archivo auxiliar a RAM
        with open(self.aux_path, "rb") as fa:
            n_aux = self._read_count(fa)
            for i in range(n_aux):
                r = self._read_record(fa, i)
                if r and not r.is_deleted:
                    r.next = NO_NEXT
                    aux_recs.append(r)
        aux_recs.sort(key=lambda x: x.listing_id)

        # 2. Mezclamos (Merge) el archivo Principal página por página hacia un Temporal
        tmp_path = self.main_path + ".tmp"
        self._init_file(tmp_path)

        with open(self.main_path, "rb") as fm, open(tmp_path, "r+b") as ft:
            n_main = self._read_count(fm)
            total_pages = 1 + ((n_main + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE)
            
            new_n_main = 0
            out_page = bytearray(PAGE_SIZE)
            out_recs_in_page = 0
            out_page_no = 1

            def flush_out_page():
                nonlocal out_page, out_recs_in_page, out_page_no
                if out_recs_in_page > 0:
                    ft.seek(out_page_no * PAGE_SIZE)
                    ft.write(out_page)
                    self.disk_writes += 1
                    out_page = bytearray(PAGE_SIZE)
                    out_recs_in_page = 0
                    out_page_no += 1

            def write_to_tmp(rec):
                nonlocal out_page, out_recs_in_page, new_n_main
                rec.next = NO_NEXT
                out_page[out_recs_in_page * RECORD_SIZE : (out_recs_in_page + 1) * RECORD_SIZE] = rec.pack()
                out_recs_in_page += 1
                new_n_main += 1
                if out_recs_in_page == RECORDS_PER_PAGE:
                    flush_out_page()

            for page_no in range(1, total_pages):
                fm.seek(page_no * PAGE_SIZE)
                page_data = fm.read(PAGE_SIZE)
                self.disk_reads += 1

                for i in range(RECORDS_PER_PAGE):
                    idx = ((page_no - 1) * RECORDS_PER_PAGE) + i
                    if idx >= n_main: break

                    rec_data = page_data[i * RECORD_SIZE : (i + 1) * RECORD_SIZE]
                    rec = Record.unpack(rec_data)

                    if not rec.is_deleted:
                        # Insertamos ordenadamente los de Auxiliar que sean menores
                        while aux_recs and aux_recs[0].listing_id < rec.listing_id:
                            write_to_tmp(aux_recs.pop(0))
                        write_to_tmp(rec)

            # Escribir los registros sobrantes del auxiliar
            for rec in aux_recs:
                write_to_tmp(rec)

            flush_out_page()
            self._write_count(ft, new_n_main)

        os.replace(tmp_path, self.main_path)
        self._init_file(self.aux_path)


    def load_csv(self, csv_path: str):
        
        records = []
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                try:
                    price_val = row.get("price", "0").strip()
                    rec = Record(
                        listing_id   = int(row["listing_id"]),
                        name         = row.get("name", ""),
                        city         = row.get("city", ""),
                        latitude     = float(row.get("latitude", 0)),
                        longitude    = float(row.get("longitude", 0)),
                        price        = int(float(price_val)) if price_val else 0,
                        room_type    = row.get("room_type", ""),
                        accommodates = int(row.get("accommodates", 0)),
                    )
                    records.append(rec)
                except (ValueError, KeyError):
                    continue

        records.sort(key=lambda r: r.listing_id)
        
        with open(self.main_path, "wb") as f:
            header_page = bytearray(PAGE_SIZE)
            struct.pack_into(HEADER_FORMAT, header_page, 0, len(records))
            f.write(bytes(header_page))

            page_buf = bytearray(PAGE_SIZE)
            recs_in_page = 0
            for rec in records:
                page_buf[recs_in_page * RECORD_SIZE : (recs_in_page + 1) * RECORD_SIZE] = rec.pack()
                recs_in_page += 1
                if recs_in_page == RECORDS_PER_PAGE:
                    f.write(bytes(page_buf))
                    page_buf = bytearray(PAGE_SIZE)
                    recs_in_page = 0
            if recs_in_page > 0:
                f.write(bytes(page_buf))

        self._init_file(self.aux_path)
        print(f"[load_csv] {len(records)} reg. cargados paginadamente.")
