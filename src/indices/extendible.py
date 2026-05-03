import struct
import time
import os
from src.indices.base_index import BaseIndex
from src.buffer_manager import BufferManager

class ExtendibleHash(BaseIndex):
    def __init__(self, table_name, page_size=4096):
        super().__init__(table_name)
        if not os.path.exists("data"): os.makedirs("data")
        self.data_file = f"data/{table_name}.dat"
        self.dir_file = f"data/{table_name}_dir.dat"
        self.buffer = BufferManager(self.data_file, page_size)
        
        self.page_size = page_size
        self.header_size = 12 # local_depth, count, next_overflow
        self.record_size = 8  # key(int 4b), value(int 4b)
        self.max_records = (self.page_size - self.header_size) // self.record_size
        
        self.global_depth = 1
        self.directory = []
        self.next_free_page = 0
        self._load_directory()

    def _load_directory(self):
        if os.path.exists(self.dir_file):
            with open(self.dir_file, 'rb') as f:
                self.global_depth = struct.unpack('i', f.read(4))[0]
                self.next_free_page = struct.unpack('i', f.read(4))[0]
                content = f.read()
                self.directory = list(struct.unpack(f'{"i" * (len(content)//4)}', content))
        else:
            self.global_depth = 1
            self.directory = [0, 1]
            self.next_free_page = 2
            self._write_empty_bucket(0, 1)
            self._write_empty_bucket(1, 1)
            self._save_directory()

    def _save_directory(self):
        with open(self.dir_file, 'wb') as f:
            f.write(struct.pack('i', self.global_depth))
            f.write(struct.pack('i', self.next_free_page))
            f.write(struct.pack(f'{"i" * len(self.directory)}', *self.directory))

    def _write_empty_bucket(self, page_id, local_depth, next_overflow=-1):
        header = struct.pack('iii', local_depth, 0, next_overflow)
        self.buffer.write_page(page_id, header)

    def _get_bucket_index(self, key):
        # Usamos los bits menos significativos del hash
        return key & ((1 << self.global_depth) - 1)

    def search(self, key) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        
        dir_idx = self._get_bucket_index(key)
        page_id = self.directory[dir_idx]
        
        result = None
        while page_id != -1:
            data = self.buffer.read_page(page_id)
            _, count, next_ov = struct.unpack('iii', data[:12])
            for i in range(count):
                off = self.header_size + (i * self.record_size)
                k, v = struct.unpack('ii', data[off:off+8])
                if k == key:
                    result = v
                    break
            if result is not None: break
            page_id = next_ov

        return self._format_result(result, self.buffer.get_io_cost(), (time.time()-start_time)*1000)

    def add(self, key, value) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        
        self._internal_add(key, value)
        
        return self._format_result(True, self.buffer.get_io_cost(), (time.time()-start_time)*1000)

    def _internal_add(self, key, value):
        dir_idx = self._get_bucket_index(key)
        page_id = self.directory[dir_idx]
        
        if not self._try_insert(page_id, key, value):
            data = self.buffer.read_page(page_id)
            local_depth = struct.unpack('i', data[:4])[0]
            
            if local_depth < self.global_depth:
                self._split_bucket(page_id)
                self._internal_add(key, value)
            else:
                # Si d == D, verificamos si ya hay un overflow (K=1 permitido)
                next_ov = struct.unpack('i', data[8:12])[0]
                if next_ov == -1:
                    new_ov_id = self.next_free_page
                    self.next_free_page += 1
                    self._write_empty_bucket(new_ov_id, local_depth)
                    # Actualizar puntero de overflow en la página principal
                    updated_header = data[:8] + struct.pack('i', new_ov_id)
                    self.buffer.write_page(page_id, updated_header + data[12:])
                    self._try_insert(new_ov_id, key, value)
                else:
                    # Ya existe un overflow y está lleno, duplicar directorio
                    self.global_depth += 1
                    self.directory = self.directory + self.directory
                    self._save_directory()
                    self._internal_add(key, value)

    def _try_insert(self, page_id, key, value):
        data = bytearray(self.buffer.read_page(page_id))
        depth, count, next_ov = struct.unpack('iii', data[:12])
        if count < self.max_records:
            off = self.header_size + (count * self.record_size)
            data[off:off+8] = struct.pack('ii', key, value)
            data[4:8] = struct.pack('i', count + 1)
            self.buffer.write_page(page_id, bytes(data))
            return True
        return False

    def _split_bucket(self, old_page_id):
        old_data = self.buffer.read_page(old_page_id)
        d_local, count, next_ov = struct.unpack('iii', old_data[:12])
        
        # Extraer registros actuales
        records = []
        for i in range(count):
            off = self.header_size + (i * self.record_size)
            records.append(struct.unpack('ii', old_data[off:off+8]))
        
        # Crear nueva página
        new_page_id = self.next_free_page
        self.next_free_page += 1
        new_d = d_local + 1
        
        self._write_empty_bucket(old_page_id, new_d)
        self._write_empty_bucket(new_page_id, new_d)
        
        # Re-mapear directorio
        bit = 1 << d_local
        for i in range(len(self.directory)):
            if self.directory[i] == old_page_id:
                if i & bit:
                    self.directory[i] = new_page_id
        self._save_directory()
        
        # Re-insertar registros
        for k, v in records:
            idx = self._get_bucket_index(k)
            self._try_insert(self.directory[idx], k, v)

    def remove(self, key) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        # Lógica de eliminación simplificada
        idx = self._get_bucket_index(key)
        page_id = self.directory[idx]
        found = False
        # (Aquí va la lógica de búsqueda y re-escritura de página sin el registro k)
        return self._format_result(found, self.buffer.get_io_cost(), (time.time()-start_time)*1000)

    def range_search(self, b, e): return {"error": "Hash no soporta rangos"}