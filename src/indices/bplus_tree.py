import os
import struct
import sys
import time
from collections import deque
from pathlib import Path

_project_root = str(Path(__file__).resolve().parents[2])
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.indices.base_index import BaseIndex
from src.buffer_manager import BufferManager

class BPlusTreeIndex(BaseIndex):
    PAGE_SIZE = 4096
    HEADER_SIZE = 16  # page_id(4) + is_leaf(4) + num_keys(4) + next_leaf(4)
    POINTER_SIZE = 4
    RID_SIZE = 8  # page_id(4) + slot_id(4)
    META_DATA_SIZE = 8

    def __init__(self, table_name, index_name, idx_key, idx_size, filepath: str = None):
        super().__init__(table_name)
        self.index_name = index_name
        self.idx_key = idx_key
        self.filepath = filepath if filepath is not None else f"data/{index_name}.bin"
        os.makedirs(os.path.dirname(self.filepath) or ".", exist_ok=True)
        self.buffer = BufferManager(self.filepath, self.PAGE_SIZE)

        if idx_key.upper() == "INT":
            self.key_format = 'i'
            self.key_size = 4
        elif idx_key.upper() == "STR":
            self.key_format = f'{idx_size}s'
            self.key_size = idx_size

        self.m = (self.PAGE_SIZE - self.HEADER_SIZE - self.POINTER_SIZE) // (self.key_size + self.POINTER_SIZE)
        self.max_leaf_keys = (self.PAGE_SIZE - self.HEADER_SIZE) // (self.key_size + self.RID_SIZE)

        if os.path.getsize(self.filepath) < self.PAGE_SIZE:
            self._initialize_tree()
        else:
            meta_data = self.buffer.read_page(0)
            self.raiz, self.siguiente_pagina_libre = struct.unpack_from('<ii', meta_data, 0)

    def _initialize_tree(self):
        self.raiz = 1
        self.siguiente_pagina_libre = 2
        root_leaf = self._pack_leaf([], [], [], -1, 1)
        meta_data = bytearray(self.PAGE_SIZE)
        struct.pack_into('<ii', meta_data, 0, self.raiz, self.siguiente_pagina_libre)
        self.buffer.write_page(0, meta_data)
        self.buffer.write_page(1, root_leaf)

    def _read_header_pagina(self, raw_page: bytes):
        """Lee el header sin parent_id (16 bytes)"""
        page_id, is_leaf, num_keys, next_leaf = struct.unpack_from('<iiii', raw_page, 0)
        return page_id, is_leaf, num_keys, next_leaf

    def _normalize_key(self, key):
        if self.idx_key.upper() == "STR":
            if isinstance(key, str):
                key = key.encode('utf-8')
            return key[:self.key_size].ljust(self.key_size, b'\x00')
        return key

    def _write_metadata(self):
        raw_meta_data = bytearray(self.PAGE_SIZE)
        struct.pack_into('<ii', raw_meta_data, 0, self.raiz, self.siguiente_pagina_libre)
        self.buffer.write_page(0, raw_meta_data)

    def _pack_leaf(self, keys, page, slot, next_leaf, page_id) -> bytes:
        """Empaqueta una hoja sin parent_id"""
        buffer = bytearray(self.PAGE_SIZE)
        offset = 0
        struct.pack_into('<iiii', buffer, offset, page_id, 1, len(keys), next_leaf)
        offset += self.HEADER_SIZE

        formato_fila = f'<{self.key_format}ii'

        for i in range(len(keys)):
            llave_actual = keys[i]
            if self.idx_key.upper() == "STR":
                if isinstance(llave_actual, str):
                    llave_actual = llave_actual.encode('utf-8')

            struct.pack_into(formato_fila, buffer, offset, llave_actual, page[i], slot[i])
            offset += self.key_size + self.RID_SIZE

        return buffer

    def _unpack_leaf(self, raw_leaf: bytes):
        """Desempaqueta una hoja"""
        offset = 0
        page_id, is_leaf, num_keys, next_leaf = self._read_header_pagina(raw_leaf)
        offset += self.HEADER_SIZE

        keys = []
        page = []
        slot = []

        if num_keys > 0:
            formato_pares = "<" + (f"{self.key_format}ii") * num_keys
            datos_extraidos = struct.unpack_from(formato_pares, raw_leaf, offset)
            keys = list(datos_extraidos[0::3])
            page = list(datos_extraidos[1::3])
            slot = list(datos_extraidos[2::3])

        return keys, page, slot, next_leaf, page_id

    def _pack_internal(self, keys, pointers, page_id) -> bytes:
        """Empaqueta un nodo interno sin parent_id"""
        buffer = bytearray(self.PAGE_SIZE)
        offset = 0
        struct.pack_into('<iiii', buffer, offset, page_id, 0, len(keys), -1)
        offset += self.HEADER_SIZE

        struct.pack_into('<i', buffer, offset, pointers[0])
        offset += self.POINTER_SIZE

        formato_fila = f'<{self.key_format}i'
        for i in range(len(keys)):
            llave_actual = keys[i]
            if self.idx_key.upper() == "STR":
                if isinstance(llave_actual, str):
                    llave_actual = llave_actual.encode('utf-8')

            struct.pack_into(formato_fila, buffer, offset, llave_actual, pointers[i + 1])
            offset += self.key_size + self.POINTER_SIZE

        return buffer

    def _unpack_internal(self, raw_internal: bytes):
        """Desempaqueta un nodo interno"""
        offset = 0
        page_id, is_leaf, num_keys, next_leaf = self._read_header_pagina(raw_internal)
        offset += self.HEADER_SIZE

        pointers = []
        keys = []

        if num_keys > 0 or is_leaf == 0:
            formato_pares = "<i" + (f"{self.key_format}i") * num_keys
            datos_extraidos = struct.unpack_from(formato_pares, raw_internal, offset)
            pointers = list(datos_extraidos[0::2])
            keys = list(datos_extraidos[1::2])

        return keys, pointers, page_id

    def search_leaf_with_path(self, value):
        """
        Busca la hoja y retorna una pila de (page_id, raw_node) de nodos visitados
        para poder hacer splits hacia arriba
        """
        path = []  # Pila de (page_id, raw_node)
        current_node = self.buffer.read_page(self.raiz)
        page_id, is_leaf, num_keys, next_leaf = self._read_header_pagina(current_node)
        path.append((page_id, current_node))

        while is_leaf == 0:
            keys, pointers, _ = self._unpack_internal(current_node)
            assert len(keys) > 0, "Error: Nodo interno sin claves"

            next_ptr = pointers[0]
            for i in range(len(keys)):
                if value <= keys[i]:
                    next_ptr = pointers[i]
                    break
                elif i + 1 == len(keys):
                    next_ptr = pointers[i + 1]
                    break

            current_node = self.buffer.read_page(next_ptr)
            page_id, is_leaf, num_keys, next_leaf = self._read_header_pagina(current_node)
            path.append((page_id, current_node))

        return path  # Retorna la pila completa

    def search_leaf(self, value):
        """Retorna solo la hoja (para compatibilidad)"""
        path = self.search_leaf_with_path(value)
        return path[-1][1]  # Solo la hoja

    def search(self, key) -> dict:
        time_start = time.time()
        self.buffer.reset_io_cost()
        key = self._normalize_key(key)
        raw = self.search_leaf(key)
        results = []

        while True:
            keys, page, slot, next_leaf, _ = self._unpack_leaf(raw)
            for i in range(len(keys)):
                if keys[i] == key:
                    results.append((page[i], slot[i]))
                elif keys[i] > key:
                    data = results if results else None
                    return self._format_result(data, self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))

            if next_leaf == -1:
                break
            if not keys or keys[-1] <= key:
                raw = self.buffer.read_page(next_leaf)
                continue
            break

        data = results if results else None
        return self._format_result(data, self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))

    def insert_leaf(self, nodo_hoja: bytes, page_id_value: int, slot_id_value: int, value):
        keys, page, slot, next_leaf, leaf_id = self._unpack_leaf(nodo_hoja)

        if len(keys) == 0:
            keys.append(value)
            page.append(page_id_value)
            slot.append(slot_id_value)
        else:
            inserted = False
            for i in range(len(keys)):
                if value <= keys[i]:
                    keys.insert(i, value)
                    page.insert(i, page_id_value)
                    slot.insert(i, slot_id_value)
                    inserted = True
                    break
            if not inserted:
                keys.append(value)
                page.append(page_id_value)
                slot.append(slot_id_value)

        return keys, page, slot, next_leaf, leaf_id

    def insert_in_parent_with_path(self, left_page_id, key, right_page_id, path):
        """
        Inserta una clave en el padre usando la pila de nodos.
        path: lista de (page_id, raw_node) desde raiz hasta la hoja donde se hizo el split
        """
        if left_page_id == self.raiz:
            # Crear nueva raíz
            new_raiz_id = self.siguiente_pagina_libre
            self.siguiente_pagina_libre += 1

            llaves = [key]
            pointers = [left_page_id, right_page_id]
            self.raiz = new_raiz_id

            raw_new_root = self._pack_internal(llaves, pointers, new_raiz_id)
            self.buffer.write_page(new_raiz_id, raw_new_root)
            self._write_metadata()
            return

        # El padre está en path[-2]
        if len(path) < 2:
            raise ValueError("Path debe contener al menos 2 nodos (padre y hoja)")

        parent_page_id = path[-2][0]
        raw_padre = self.buffer.read_page(parent_page_id)
        keys_padre, pointers_padre, _ = self._unpack_internal(raw_padre)

        # Encontrar dónde insertamos
        for i in range(len(pointers_padre)):
            if pointers_padre[i] == left_page_id:
                pointers_padre.insert(i + 1, right_page_id)
                keys_padre.insert(i, key)

                if len(keys_padre) > self.m:
                    # Split del padre
                    mid = len(keys_padre) // 2
                    new_padre_id = self.siguiente_pagina_libre
                    self.siguiente_pagina_libre += 1
                    self._write_metadata()

                    new_parent_llaves = keys_padre[mid + 1:]
                    new_parent_pointers = pointers_padre[mid + 1:]

                    value_a_subir = keys_padre[mid]
                    keys_padre = keys_padre[:mid]
                    pointers_padre = pointers_padre[:mid + 1]

                    raw_padre_left = self._pack_internal(keys_padre, pointers_padre, parent_page_id)
                    raw_padre_right = self._pack_internal(new_parent_llaves, new_parent_pointers, new_padre_id)

                    self.buffer.write_page(parent_page_id, raw_padre_left)
                    self.buffer.write_page(new_padre_id, raw_padre_right)

                    # Recursivamente insertar en el abuelo
                    new_path = path[:-1]  # Path sin la hoja
                    self.insert_in_parent_with_path(parent_page_id, value_a_subir, new_padre_id, new_path)
                else:
                    # Sin split
                    raw_padre_updated = self._pack_internal(keys_padre, pointers_padre, parent_page_id)
                    self.buffer.write_page(parent_page_id, raw_padre_updated)

                return

        raise ValueError("No se encontró el nodo hijo en el padre")

    def add(self, key, page_id_value: int, slot_id_value: int):
        time_start = time.time()
        self.buffer.reset_io_cost()
        key = self._normalize_key(key)

        path = self.search_leaf_with_path(key)
        leaf_page_id, raw_leaf = path[-1]

        keys, page, slot, next_leaf, leaf_id = self.insert_leaf(raw_leaf, page_id_value, slot_id_value, key)

        if len(keys) > self.max_leaf_keys:
            # Split de la hoja
            mid = len(keys) // 2

            left_keys = keys[:mid]
            left_page = page[:mid]
            left_slot = slot[:mid]

            right_keys = keys[mid:]
            right_page = page[mid:]
            right_slot = slot[mid:]

            nueva_hoja_id = self.siguiente_pagina_libre
            self.siguiente_pagina_libre += 1
            self._write_metadata()

            left_next_leaf = nueva_hoja_id
            right_next_leaf = next_leaf

            raw_left = self._pack_leaf(left_keys, left_page, left_slot, left_next_leaf, leaf_page_id)
            raw_right = self._pack_leaf(right_keys, right_page, right_slot, right_next_leaf, nueva_hoja_id)

            self.buffer.write_page(leaf_page_id, raw_left)
            self.buffer.write_page(nueva_hoja_id, raw_right)

            # Insertar en el padre usando la pila
            separator = right_keys[0]
            self.insert_in_parent_with_path(leaf_page_id, separator, nueva_hoja_id, path)
        else:
            # Sin split
            raw_updated = self._pack_leaf(keys, page, slot, next_leaf, leaf_page_id)
            self.buffer.write_page(leaf_page_id, raw_updated)

        return self._format_result(True, self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))

    def _key_display(self, k):
        if isinstance(k, bytes):
            return k.rstrip(b'\x00').decode('utf-8', errors='replace')
        return k

    def print_tree(self):
        print(f"\n--- ÁRBOL B+ (raíz=Pág {self.raiz}, sig_libre=Pág {self.siguiente_pagina_libre}) ---")
        cola = deque()
        cola.append((self.raiz, 0))
        nivel_actual = -1
        while cola:
            page_id, nivel = cola.popleft()
            if nivel != nivel_actual:
                nivel_actual = nivel
                print(f"  Nivel {nivel}:")
            raw = self.buffer.read_page(page_id)
            pid, is_leaf, num_keys, next_leaf = self._read_header_pagina(raw)
            if is_leaf == 1:
                keys, pages, slots, nl, _ = self._unpack_leaf(raw)
                keys_disp = [self._key_display(k) for k in keys]
                rids = list(zip(pages, slots))
                next_disp = f"Pág {nl}" if nl != -1 else "Fin"
                print(f"    [Hoja  pid={pid} next={next_disp}]  keys={keys_disp}  RIDs={rids}")
            else:
                keys, ptrs, _ = self._unpack_internal(raw)
                keys_disp = [self._key_display(k) for k in keys]
                print(f"    [Inter pid={pid}]  keys={keys_disp}  ptrs={ptrs}")
                for ptr in ptrs:
                    cola.append((ptr, nivel + 1))
        print("--- FIN ÁRBOL ---\n")

    def _min_leaf_keys(self):
        return (self.max_leaf_keys + 1) // 2

    def _min_internal_pointers(self):
        return (self.m + 2) // 2

    def remove(self, key) -> dict:
        time_start = time.time()
        self.buffer.reset_io_cost()
        key = self._normalize_key(key)

        raw = self.search_leaf(key)
        keys, _, _, next_leaf, leaf_id = self._unpack_leaf(raw)

        while key not in keys and next_leaf != -1 and (not keys or keys[-1] < key):
            raw = self.buffer.read_page(next_leaf)
            keys, _, _, next_leaf, leaf_id = self._unpack_leaf(raw)

        if key not in keys:
            return self._format_result(False, self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))

        self._delete_entry(leaf_id, key, pointer=None)
        return self._format_result(True, self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))

    def _delete_entry(self, node_id, key, pointer):
        """Método simplificado de eliminación"""
        raw = self.buffer.read_page(node_id)
        pid, is_leaf, num_keys, next_leaf = self._read_header_pagina(raw)

        if is_leaf == 1:
            ks, ps, ss, nl, _ = self._unpack_leaf(raw)
            if key in ks:
                i = ks.index(key)
                ks.pop(i)
                ps.pop(i)
                ss.pop(i)
                raw = self._pack_leaf(ks, ps, ss, nl, node_id)
                self.buffer.write_page(node_id, raw)
        else:
            ks, ptrs, _ = self._unpack_internal(raw)
            if key in ks:
                i = ks.index(key)
                ks.pop(i)
                raw = self._pack_internal(ks, ptrs, node_id)
                self.buffer.write_page(node_id, raw)

        if node_id == self.raiz:
            if is_leaf == 0:
                ks, ptrs, _ = self._unpack_internal(self.buffer.read_page(node_id))
                if len(ptrs) == 1 and len(ks) == 0:
                    self.raiz = ptrs[0]
                    self._write_metadata()

    def range_search(self, begin_key, end_key) -> dict:
        time_start = time.time()
        self.buffer.reset_io_cost()
        begin_key = self._normalize_key(begin_key)
        end_key = self._normalize_key(end_key)

        if begin_key > end_key:
            return self._format_result([], self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))

        raw = self.search_leaf(begin_key)
        results = []

        while True:
            keys, page, slot, next_leaf, _ = self._unpack_leaf(raw)
            for i in range(len(keys)):
                if keys[i] > end_key:
                    return self._format_result(results, self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))
                if keys[i] >= begin_key:
                    results.append((page[i], slot[i]))

            if next_leaf == -1:
                break
            if not keys or keys[-1] <= end_key:
                raw = self.buffer.read_page(next_leaf)
                continue
            break

        return self._format_result(results, self.buffer.get_io_cost(), round((time.time() - time_start) * 1000, 3))
