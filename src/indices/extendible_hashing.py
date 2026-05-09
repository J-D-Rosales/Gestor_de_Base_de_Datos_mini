import struct
import time
import os
import hashlib
from src.indices.base_index import BaseIndex
from src.buffer_manager import BufferManager

class ExtendibleHashing(BaseIndex):
    def __init__(self, table_name, index_name, key_type, key_size=30, page_size=4096,
                 filepath: str = None):
        super().__init__(table_name)
        self.index_name = index_name
        self.key_type = key_type.upper()
        self.page_size = page_size

        if self.key_type in ["INT"]:
            self.key_fmt = 'i'
            self.k_size = 4
        elif self.key_type in ["FLOAT"]:
            self.key_fmt = 'f'
            self.k_size = 4
        elif self.key_type in ["VARCHAR", "STR"]:
            self.key_fmt = f'{key_size}s'
            self.k_size = key_size
        else:
            raise ValueError(f"Tipo de dato {key_type} no soportado por el Hash.")

        # Registro: Llave + Puntero (int 4 bytes)
        self.record_format = f"={self.key_fmt}i"
        self.record_size = struct.calcsize(self.record_format)

        # Header: local_depth(i), count(i), next_overflow(i) = 12 bytes
        self.header_size = 12
        self.max_records = (self.page_size - self.header_size) // self.record_size

        # 2. Archivos y Buffer
        if filepath is None:
            # Backwards-compat: ubicación legacy si nadie inyecta el path.
            if not os.path.exists("data"):
                os.makedirs("data")
            self.data_file = f"data/{index_name}.dat"
            self.dir_file = f"data/{index_name}_dir.dat"
        else:
            parent = os.path.dirname(filepath)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self.data_file = filepath
            # `<path>.dat` → `<path>_dir.dat`; en general `<path>` → `<path>_dir`.
            if filepath.endswith(".dat"):
                self.dir_file = filepath[:-4] + "_dir.dat"
            else:
                self.dir_file = filepath + "_dir"
        self.buffer = BufferManager(self.data_file, self.page_size)
        
        self.global_depth = 1
        self.directory = []
        self.next_free_page = 0
        self._load_directory()

    def _get_bucket_index(self, key) -> int:
        """Hash universal usando SHA-256 para soportar STR, FLOAT e INT."""
        # Normalización de la llave para el hash
        key_str = str(key).encode('utf-8')
        hash_val = hashlib.sha256(key_str).digest()
        # Convertimos los primeros 4 bytes del digest en un entero
        hash_int = int.from_bytes(hash_val[:4], byteorder='big')
        return hash_int & ((1 << self.global_depth) - 1)

    def _load_directory(self):
        if os.path.exists(self.dir_file):
            with open(self.dir_file, 'rb') as f:
                self.global_depth = struct.unpack('i', f.read(4))[0]
                self.next_free_page = struct.unpack('i', f.read(4))[0]
                content = f.read()
                if content:
                    self.directory = list(struct.unpack(f'{"i" * (len(content)//4)}', content))
        else:
            self.global_depth = 1
            self.directory = [0, 1]
            self.next_free_page = 2
            self._write_empty_bucket(0, 1) # Bucket 0, d_local 1
            self._write_empty_bucket(1, 1) # Bucket 1, d_local 1
            self._save_directory()

    def _save_directory(self):
        with open(self.dir_file, 'wb') as f:
            f.write(struct.pack('i', self.global_depth))
            f.write(struct.pack('i', self.next_free_page))
            f.write(struct.pack(f'{"i" * len(self.directory)}', *self.directory))

    def _write_empty_bucket(self, page_id, local_depth, next_ov=-1):
        header = struct.pack('iii', local_depth, 0, next_ov)
        self.buffer.write_page(page_id, header)

    def search(self, key) -> dict:
        start_t = time.time()
        self.buffer.reset_io_cost()
        
        idx = self._get_bucket_index(key)
        page_id = self.directory[idx]
        
        result = None
        while page_id != -1:
            data = self.buffer.read_page(page_id)
            d_local, count, next_ov = struct.unpack('iii', data[:12])
            
            for i in range(count):
                off = self.header_size + (i * self.record_size)
                # Unpack dinámico según el tipo de clave
                k_raw, val = struct.unpack(self.record_format, data[off:off+self.record_size])
                
                # Si es string, limpiar bytes nulos
                k_decoded = k_raw.decode('utf-8').strip('\x00') if isinstance(k_raw, bytes) else k_raw
                
                if str(k_decoded) == str(key): # Comparación segura
                    result = val
                    break
            
            if result is not None: break
            page_id = next_ov

        return self._format_result(result, self.buffer.get_io_cost(), (time.time()-start_t)*1000)

    def search_all(self, key) -> dict:
        start_t = time.time()
        self.buffer.reset_io_cost()

        idx = self._get_bucket_index(key)
        page_id = self.directory[idx]

        results = []
        while page_id != -1:
            data = self.buffer.read_page(page_id)
            d_local, count, next_ov = struct.unpack('iii', data[:12])

            for i in range(count):
                off = self.header_size + (i * self.record_size)
                k_raw, val = struct.unpack(self.record_format, data[off:off+self.record_size])
                k_decoded = k_raw.decode('utf-8').strip('\x00') if isinstance(k_raw, bytes) else k_raw

                if str(k_decoded) == str(key):
                    results.append(val)

            page_id = next_ov

        return self._format_result(results, self.buffer.get_io_cost(), (time.time()-start_t)*1000)

    def add(self, key, value) -> dict:
        start_t = time.time()
        self.buffer.reset_io_cost()
        
        # Asegurar tipo correcto antes de insertar
        try:
            if self.key_type == "INT": key = int(key)
            elif self.key_type == "FLOAT": key = float(key)
        except:
            return {"error": f"Invalid type for key: {key}"}

        self._internal_insert(key, value)
        return self._format_result(True, self.buffer.get_io_cost(), (time.time()-start_t)*1000)

    def _internal_insert(self, key, value):
        idx = self._get_bucket_index(key)
        page_id = self.directory[idx]
        
        if not self._try_push_to_bucket(page_id, key, value):
            # No hay espacio. Lógica de Laboratorio: d_local < Global Depth?
            data = self.buffer.read_page(page_id)
            d_local = struct.unpack('i', data[:4])[0]
            
            if d_local < self.global_depth:
                self._split_bucket(page_id)
                self._internal_insert(key, value)
            else:
                # d_local == Global Depth. ¿Ya hay overflow encadenado?
                next_ov = struct.unpack('i', data[8:12])[0]
                if next_ov == -1:
                    # Crear el primer overflow del bucket
                    ov_id = self.next_free_page
                    self.next_free_page += 1
                    self._write_empty_bucket(ov_id, d_local)
                    # Actualizar puntero en bucket original
                    new_header = struct.pack('iii', d_local, struct.unpack('i', data[4:8])[0], ov_id)
                    self.buffer.write_page(page_id, new_header + data[12:])
                    self._try_push_to_bucket(ov_id, key, value)
                else:
                    # Recorrer la cadena de overflow hasta el último nodo y
                    # empujar ahí. Si está lleno, anexar un nuevo overflow.
                    # Esto evita la recursión infinita cuando muchas keys
                    # hashean al mismo bucket (ej. 'Lima' x 100k); en ese
                    # caso duplicar el directorio no redistribuye.
                    tail_id = next_ov
                    while True:
                        tail_data = self.buffer.read_page(tail_id)
                        _, _, t_next = struct.unpack('iii', tail_data[:12])
                        if t_next == -1:
                            break
                        tail_id = t_next
                    if not self._try_push_to_bucket(tail_id, key, value):
                        new_ov_id = self.next_free_page
                        self.next_free_page += 1
                        self._write_empty_bucket(new_ov_id, d_local)
                        # Linkear tail.next_ov = new_ov_id
                        tail_data = self.buffer.read_page(tail_id)
                        td_local, tcount, _ = struct.unpack('iii', tail_data[:12])
                        new_header = struct.pack('iii', td_local, tcount, new_ov_id)
                        self.buffer.write_page(tail_id, new_header + tail_data[12:])
                        self._try_push_to_bucket(new_ov_id, key, value)

    def _try_push_to_bucket(self, page_id, key, value) -> bool:
        data = bytearray(self.buffer.read_page(page_id))
        d_local, count, next_ov = struct.unpack('iii', data[:12])
        
        if count < self.max_records:
            off = self.header_size + (count * self.record_size)
            
            # Preparar la llave para struct
            k_to_pack = key.encode('utf-8')[:self.k_size] if isinstance(key, str) else key
            
            data[off:off+self.record_size] = struct.pack(self.record_format, k_to_pack, value)
            data[4:8] = struct.pack('i', count + 1)
            self.buffer.write_page(page_id, bytes(data))
            return True
        return False

    def _split_bucket(self, old_page_id):
        old_data = self.buffer.read_page(old_page_id)
        d_local, count, next_ov = struct.unpack('iii', old_data[:12])
        
        records = []
        
        # 1. Extraer registros del bucket principal usando tamaños dinámicos
        for i in range(count):
            off = self.header_size + (i * self.record_size)
            k_raw, v = struct.unpack(self.record_format, old_data[off:off+self.record_size])
            # Limpiar bytes nulos si es string
            k_decoded = k_raw.decode('utf-8').strip('\x00') if isinstance(k_raw, bytes) else k_raw
            records.append((k_decoded, v))
            
        # 2. Extraer registros del bucket de overflow si existe (K=1)
        if next_ov != -1:
            ov_data = self.buffer.read_page(next_ov)
            _, ov_count, _ = struct.unpack('iii', ov_data[:12])
            for i in range(ov_count):
                off = self.header_size + (i * self.record_size)
                k_raw, v = struct.unpack(self.record_format, ov_data[off:off+self.record_size])
                k_decoded = k_raw.decode('utf-8').strip('\x00') if isinstance(k_raw, bytes) else k_raw
                records.append((k_decoded, v))
        
        # 3. Crear nueva página
        new_page_id = self.next_free_page
        self.next_free_page += 1
        new_d = d_local + 1
        
        self._write_empty_bucket(old_page_id, new_d)
        self._write_empty_bucket(new_page_id, new_d)
        
        # 4. Re-mapear directorio
        bit = 1 << d_local
        for i in range(len(self.directory)):
            if self.directory[i] == old_page_id:
                if i & bit:
                    self.directory[i] = new_page_id
        self._save_directory()
        
        # 5. Re-insertar registros
        for k, v in records:
            # Llamamos a _internal_insert para que el algoritmo lo ubique y vuelva a 
            # gestionar overflows si por casualidad muchos caen en el mismo nuevo bucket
            self._internal_insert(k, v)
            
            
    def remove(self, key) -> dict:
            start_time = time.time()
            self.buffer.reset_io_cost()
            
            idx = self._get_bucket_index(key)
            page_id = self.directory[idx]
            found = False
            
            curr_id = page_id
            while curr_id != -1:
                data = self.buffer.read_page(curr_id)
                depth, count, next_ov = struct.unpack('iii', data[:12])
                
                new_records = []
                page_found = False
                
                # Revisar cada registro
                for i in range(count):
                    off = self.header_size + (i * self.record_size)
                    k_raw, v = struct.unpack(self.record_format, data[off:off+self.record_size])
                    k_decoded = k_raw.decode('utf-8').strip('\x00') if isinstance(k_raw, bytes) else k_raw
                    
                    # Si lo encontramos, lo omitimos (no lo guardamos en new_records)
                    if str(k_decoded) == str(key):
                        found = True
                        page_found = True
                    else:
                        new_records.append((k_raw, v)) # Guardamos el raw para empaquetarlo rápido luego
                
                # Si encontramos el registro en esta página, la reescribimos compactada
                if page_found:
                    new_data = bytearray(self.page_size)
                    # Escribimos el header actualizado (menos 1 en el count)
                    new_data[:12] = struct.pack('iii', depth, len(new_records), next_ov)
                    
                    # Escribimos los registros que sí se quedan
                    for i, (k_raw, v) in enumerate(new_records):
                        off = self.header_size + (i * self.record_size)
                        new_data[off:off+self.record_size] = struct.pack(self.record_format, k_raw, v)
                        
                    self.buffer.write_page(curr_id, bytes(new_data))
                    break # Termina el while, ya se eliminó
                    
                curr_id = next_ov

            exec_time = (time.time() - start_time) * 1000
            return self._format_result(found, self.buffer.get_io_cost(), exec_time)
        
        
    def range_search(self, begin_key, end_key) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        # Retornamos formato estándar con error
        return self._format_result({"error": "Hash Extensible no soporta búsquedas por rango."}, self.buffer.get_io_cost(), (time.time() - start_time) * 1000)