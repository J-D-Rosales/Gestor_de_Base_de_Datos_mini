import os
import time
from base_index import BaseIndex
from buffer_manager import BufferManager

# Importamos nuestros nuevos módulos limpios
from .directory import Directory
from .bucket import Bucket
from . import hash_utils

class ExtendibleHashing(BaseIndex):
    def __init__(self, table_name, index_name, key_type, key_size=50, page_size=4096):
        super().__init__(table_name)
        self.index_name = index_name
        self.key_type = key_type.upper()
        self.page_size = page_size
        
        # 1. Definimos el formato de la llave (Struct format)
        if self.key_type == "INT":
            self.key_fmt = 'i'
        elif self.key_type in ["FLOAT"]:
            self.key_fmt = 'd' # Double precision para asegurar precisión
        elif self.key_type in ["VARCHAR", "STR"]:
            self.key_fmt = f'{key_size}s'
        else:
            raise ValueError(f"Tipo de dato {key_type} no soportado.")

        # 2. Formato del registro completo: Llave + page_id(int) + slot_id(int)
        self.record_format = f"={self.key_fmt}ii"
        
        # 3. Inicializamos las dependencias de I/O y Estructura
        base_dir = "data"
        os.makedirs(base_dir, exist_ok=True)
        
        self.db_filepath = f"{base_dir}/{index_name}.bin"
        self.dir_filepath = f"{base_dir}/{index_name}.dir"
        
        self.buffer = BufferManager(self.db_filepath, self.page_size)
        self.directory = Directory(self.dir_filepath)

    def add(self, key, page_id_value, slot_id_value) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        
        # Normalizamos la llave para strings (rellenamos con null bytes)
        if self.key_type in ["VARCHAR", "STR"] and isinstance(key, str):
            key = key.encode('utf-8').ljust(int(self.key_fmt[:-1]), b'\0')

        key_hash = hash_utils.generate_hash(key)

        # Bucle que intenta insertar y divide si es necesario
        while True:
            # Obtenemos los bits relevantes según la profundidad global
            hash_suffix = hash_utils.get_bits(key_hash, self.directory.global_depth)
            target_page_id = self.directory.get_page_id(hash_suffix)
            
            # Leemos la página del disco y la convertimos a objeto Bucket
            raw_data = self.buffer.read_page(target_page_id)
            bucket = Bucket.from_bytes(raw_data, self.page_size, self.record_format)

            # Intentamos insertar (retorna False si está lleno)
            # Pasamos los 3 valores planos para que struct.pack los entienda luego
            if bucket.insert(key, page_id_value, slot_id_value):
                # Éxito: Escribimos al disco y terminamos
                self.buffer.write_page(target_page_id, bucket.to_bytes())
                break
            else:
                # Fracaso: El bucket está lleno. Procedemos al Split.
                self._split_bucket(target_page_id, bucket, key_hash)

        exec_time = (time.time() - start_time) * 1000
        return self._format_result({"status": "ok"}, self.buffer.get_io_cost(), exec_time)

    def _split_bucket(self, old_page_id: int, bucket: Bucket, key_hash: int):
        """Ejecuta la división de un bucket lleno y actualiza el directorio."""
        local_depth = bucket.local_depth
        
        # REGLA DE ORO 1: Si local == global, duplicamos directorio primero
        if local_depth == self.directory.global_depth:
            self.directory.double()

        # REGLA DE ORO 2: Aumentamos la profundidad local
        new_depth = local_depth + 1
        
        # Asignamos una nueva página al final del archivo
        new_page_id = self.buffer.num_pages()
        new_bucket = Bucket(self.page_size, self.record_format, new_depth)
        
        # Extraemos todos los registros y reseteamos el bucket original
        all_records = bucket.records[:]
        bucket.clear()
        bucket.local_depth = new_depth

        # Máscara para evaluar el nuevo bit significativo (el que define la división)
        split_mask = 1 << local_depth 

        # Redistribuimos los registros entre el viejo y el nuevo bucket
        for rec in all_records:
            rec_key = rec[0]
            rec_hash = hash_utils.generate_hash(rec_key)
            
            # Si el nuevo bit es 0, se queda; si es 1, se va al nuevo
            if (rec_hash & split_mask) == 0:
                bucket.insert(*rec)
            else:
                new_bucket.insert(*rec)

        # Extraemos el sufijo base que compartían antes del split
        base_suffix = key_hash & ((1 << local_depth) - 1)
        
        # Actualizamos masivamente todos los punteros afectados en el Directorio
        self.directory.update_pointers(base_suffix, new_depth, old_page_id, new_page_id)

        # Guardamos ambas páginas en disco vía BufferManager
        self.buffer.write_page(old_page_id, bucket.to_bytes())
        self.buffer.write_page(new_page_id, new_bucket.to_bytes())

    def search(self, key) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        
        if self.key_type in ["VARCHAR", "STR"] and isinstance(key, str):
            key = key.encode('utf-8').ljust(int(self.key_fmt[:-1]), b'\0')

        # 1. Hashear y extraer sufijo
        key_hash = hash_utils.generate_hash(key)
        hash_suffix = hash_utils.get_bits(key_hash, self.directory.global_depth)
        
        # 2. Buscar en Directorio y cargar página
        target_page_id = self.directory.get_page_id(hash_suffix)
        raw_data = self.buffer.read_page(target_page_id)
        bucket = Bucket.from_bytes(raw_data, self.page_size, self.record_format)

        # 3. Buscar en la memoria del bucket (O(1) o O(N local))
        results = []
        for rec in bucket.records:
            if rec[0] == key:
                # rec[1] = page_id_value, rec[2] = slot_id_value
                results.append((rec[1], rec[2]))

        exec_time = (time.time() - start_time) * 1000
        return self._format_result(results, self.buffer.get_io_cost(), exec_time)

    def remove(self, key) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        
        if self.key_type in ["VARCHAR", "STR"] and isinstance(key, str):
            key = key.encode('utf-8').ljust(int(self.key_fmt[:-1]), b'\0')

        key_hash = hash_utils.generate_hash(key)
        hash_suffix = hash_utils.get_bits(key_hash, self.directory.global_depth)
        target_page_id = self.directory.get_page_id(hash_suffix)
        
        raw_data = self.buffer.read_page(target_page_id)
        bucket = Bucket.from_bytes(raw_data, self.page_size, self.record_format)

        # bucket.remove elimina la primera ocurrencia encontrada
        if bucket.remove(key):
            self.buffer.write_page(target_page_id, bucket.to_bytes())
            deleted = True
        else:
            deleted = False

        exec_time = (time.time() - start_time) * 1000
        return self._format_result({"deleted": deleted}, self.buffer.get_io_cost(), exec_time)

    def range_search(self, begin_key, end_key) -> dict:
        start_time = time.time()
        self.buffer.reset_io_cost()
        # Hash Extensible no soporta búsquedas por rango.
        return self._format_result([], self.buffer.get_io_cost(), (time.time() - start_time) * 1000)
    
    
# =====================================================================
    # HERRAMIENTAS DE DEBUGGING Y VALIDACIÓN (Agregar al final de la clase)
    # =====================================================================

    def print_directory_state(self):
        """Imprime el estado actual del directorio en la consola para depuración visual."""
        print(f"\\n--- ESTADO DEL DIRECTORIO (Global Depth: {self.directory.global_depth}) ---")
        
        # Iteramos sobre todos los punteros del directorio
        for index, page_id in enumerate(self.directory.bucket_pointers):
            # Formateamos el índice a binario con ceros a la izquierda según global_depth
            bin_index = format(index, f'0{self.directory.global_depth}b')
            print(f"Index [{index:03d} | Bin: {bin_index}] -> Page ID: {page_id}")
        print("----------------------------------------------------------------\\n")

    def print_bucket_state(self, page_id: int):
        """Carga y muestra el contenido físico y metadatos de un bucket específico."""
        raw_data = self.buffer.read_page(page_id)
        bucket = Bucket.from_bytes(raw_data, self.page_size, self.record_format)
        
        print(f"--- BUCKET PAGE {page_id} ---")
        print(f"Local Depth: {bucket.local_depth}")
        
        # Mostramos la cantidad actual vs la capacidad máxima
        print(f"Records: {len(bucket.records)} / {bucket.max_records}")
        
        # Listamos las llaves contenidas para ver la distribución
        keys = [rec[0] for rec in bucket.records]
        print(f"Keys: {keys}")
        print("------------------------\\n")

    def validate_index(self):
        """
        Validador estricto de Invariantes Matemáticas del Hash Extensible.
        Si esto lanza un AssertionError (falla), hay un bug crítico en la lógica de splits.
        """
        from collections import Counter
        
        global_depth = self.directory.global_depth
        # Contamos cuántas entradas del directorio apuntan a cada Page ID
        pointer_counts = Counter(self.directory.bucket_pointers)

        for page_id, count in pointer_counts.items():
            # Cargamos el bucket para inspeccionar sus metadatos reales
            raw_data = self.buffer.read_page(page_id)
            bucket = Bucket.from_bytes(raw_data, self.page_size, self.record_format)
            local_depth = bucket.local_depth

            # INVARIANTE 1: La profundidad local jamás puede superar a la global
            assert local_depth <= global_depth, (
                f"FATAL: Page {page_id} tiene local_depth ({local_depth}) > global_depth ({global_depth})."
            )

            # INVARIANTE 2: La cantidad de punteros hacia un bucket obedece a 2^(d_global - d_local)
            expected_pointers = 1 << (global_depth - local_depth)
            assert count == expected_pointers, (
                f"FATAL: Page {page_id} (d_local={local_depth}) debería tener {expected_pointers} "
                f"punteros en d_global={global_depth}, pero tiene {count}."
            )

        print("[OK] Todas las invariantes matemáticas del índice son correctas.")
    