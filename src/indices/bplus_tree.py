import os
from base_index import BaseIndex
import struct
from buffer_manager import BufferManager
import time
from collections import deque

class BPlusTreeIndex(BaseIndex):
    PAGE_SIZE= 4096
    HEADER_SIZE= 20 #page_id(4 bytes) + is_leaf(4 bytes) + num_keys(4 bytes) + parent_id(4 bytes) + next_leaf(4 bytes, solo para hojas)
    POINTER_SIZE= 4 #page_id de un nodo hijo(4 bytes)
    RID_SIZE= 8 #page_id(4 bytes) + slot_id(4 bytes)
    META_DATA_SIZE= 8 #page_root_id(4 bytes) + sigueinte_pagina_libre(4 bytes)

    def __init__ (self, table_name, index_name, idx_key, idx_size):
        super().__init__(table_name)
        self.index_name = index_name
        self.idx_key = idx_key
        self.filepath = f"data/{index_name}.bin"
        self.buffer=BufferManager(self.filepath, self.PAGE_SIZE)

        if idx_key.upper()=="INT":
            self.key_format ='i'
            self.key_size = 4
        elif idx_key.upper()=="STR":
            self.key_format = f'{idx_size}s'
            self.key_size= idx_size

        self.m=(self.PAGE_SIZE - self.HEADER_SIZE - self.POINTER_SIZE) // (self.key_size + self.POINTER_SIZE) #orden del árbol
        self.max_leaf_keys=(self.PAGE_SIZE - self.HEADER_SIZE) // (self.key_size + self.RID_SIZE) #máximo número de claves en una hoja

        if os.path.getsize(self.filepath) < self.PAGE_SIZE:
            self._initialize_tree()
        else:
            meta_data=self.buffer.read_page(0)
            self.raiz, self.siguiente_pagina_libre=struct.unpack_from('<ii',meta_data,0)
    
    def _initialize_tree(self):
        self.raiz=1
        self.siguiente_pagina_libre=2
        root_leaf=self._pack_leaf([],[],[],-1,-1,1)
        meta_data=bytearray(self.PAGE_SIZE)
        struct.pack_into('<ii', meta_data, 0, self.raiz, self.siguiente_pagina_libre)
        self.buffer.write_page(0, meta_data)
        self.buffer.write_page(1, root_leaf)
    
    def _read_header_pagina(self,raw_page: bytes):
        page_id, is_leaf, num_keys, parent_id, next_leaf=struct.unpack_from('<iiiii',raw_page,0)
        return page_id, is_leaf, num_keys, parent_id, next_leaf

    def _normalize_key(self, key):
        if self.idx_key.upper() == "STR":
            if isinstance(key, str):
                key = key.encode('utf-8')
            return key[:self.key_size].ljust(self.key_size, b'\x00')
        return key

    def _write_metadata(self):
        raw_meta_data=bytearray(self.PAGE_SIZE)
        struct.pack_into('<ii', raw_meta_data, 0, self.raiz, self.siguiente_pagina_libre)
        self.buffer.write_page(0, raw_meta_data)

    def _pack_leaf(self,keys,page,slot,parent_id,next_leaf,page_id) -> bytes:
        buffer=bytearray(self.PAGE_SIZE)

        offset=0    
        struct.pack_into('<iiiii', buffer, offset, page_id, 1, len(keys), parent_id, next_leaf)
        offset+=self.HEADER_SIZE

        formato_fila = f'<{self.key_format}ii' #llave, page_id, slot_id

        for i in range(len(keys)):
            llave_actual=keys[i]
            if self.idx_key.upper() == "STR":
                if isinstance(llave_actual, str):
                    llave_actual = llave_actual.encode('utf-8')

            struct.pack_into(formato_fila, buffer, offset, llave_actual, page[i], slot[i])
            offset+=self.key_size+self.RID_SIZE

        return buffer

    def _unpack_leaf(self, raw_leaf: bytes):
        offset=0
        page_id, is_leaf, num_keys, parent_id, next_leaf=self._read_header_pagina(raw_leaf)
        offset+=self.HEADER_SIZE

        formato_pares="<"+(f"{self.key_format}ii")*num_keys #llave, page_id, slot_id
        datos_extraidos=struct.unpack_from(formato_pares,raw_leaf,offset)
        keys=list(datos_extraidos[0::3])
        page=list(datos_extraidos[1::3])
        slot=list(datos_extraidos[2::3])

        return keys, page, slot, parent_id, next_leaf, page_id
    
    def _pack_internal(self, keys, pointers, parent_id, page_id) -> bytes:
        buffer=bytearray(self.PAGE_SIZE)
        offset=0
        struct.pack_into('<iiiii', buffer, offset, page_id, 0, len(keys), parent_id, -1)
        offset+=self.HEADER_SIZE

        struct.pack_into('<i', buffer, offset, pointers[0])
        offset+=self.POINTER_SIZE

        formato_fila = f'<{self.key_format}i' #llave, puntero
        for i in range(len(keys)):
            llave_actual = keys[i]
            if self.idx_key.upper() == "STR":
                if isinstance(llave_actual, str):
                    llave_actual = llave_actual.encode('utf-8')

            struct.pack_into(formato_fila, buffer, offset, llave_actual, pointers[i+1])
            offset+=self.POINTER_SIZE+self.key_size
        
        return buffer
    
    def _unpack_internal(self, raw_internal: bytes):
        offset=0
        page_id, is_leaf, num_keys, parent_id, next_leaf=self._read_header_pagina(raw_internal)
        offset+=self.HEADER_SIZE

        formato_pares="<i"+(f"{self.key_format}i")*num_keys #puntero, llave, puntero
        datos_extraidos=struct.unpack_from(formato_pares,raw_internal,offset)
        pointers=list(datos_extraidos[0::2])
        keys=list(datos_extraidos[1::2])

        return keys, pointers, parent_id, page_id
    
    def search_leaf(self, value) -> bytes:
        current_node=self.buffer.read_page(self.raiz)
        page_id, is_leaf, num_keys, parent_id, next_leaf=self._read_header_pagina(current_node)

        while is_leaf==0:
            keys,pointers, parent_id, page_id=self._unpack_internal(current_node)
            assert len(keys)>0, "Error: Nodo interno sin claves"

            for i in range(len(keys)):
                if value<=keys[i]:
                    current_node=self.buffer.read_page(pointers[i])
                    break
                elif (i+1==len(keys)):
                    current_node=self.buffer.read_page(pointers[i+1])
                    break

            page_id, is_leaf, num_keys, parent_id, next_leaf=self._read_header_pagina(current_node)

        return current_node
    
    def search(self, key) -> dict:
        time_start = time.time()
        key = self._normalize_key(key)
        raw = self.search_leaf(key)
        results = []

        while True:
            keys, page, slot, _, next_leaf, _ = self._unpack_leaf(raw)
            for i in range(len(keys)):
                if keys[i] == key:
                    results.append((page[i], slot[i]))
                elif keys[i] > key:
                    data = results if results else None
                    return self._format_result(data, self.buffer.get_io_cost(), time.time() - time_start)

            if next_leaf == -1:
                break
            if not keys or keys[-1] <= key:
                raw = self.buffer.read_page(next_leaf)
                continue
            break

        data = results if results else None
        return self._format_result(data, self.buffer.get_io_cost(), time.time() - time_start)

    def insert_leaf(self, nodo_hoja: bytes, page_id_value: int, slot_id_value: int, value):

        keys,page,slot,_,_,_=self._unpack_leaf(nodo_hoja)

        if len(keys)!=0:
            temp1=keys.copy()
            for i in range(len(temp1)):
                if (value==temp1[i]):
                    keys.insert(i+1,value)
                    page.insert(i+1,page_id_value)
                    slot.insert(i+1,slot_id_value)
                    break
                elif value<temp1[i]:
                    keys=keys[:i]+[value]+keys[i:]
                    page=page[:i]+[page_id_value]+page[i:]
                    slot=slot[:i]+[slot_id_value]+slot[i:]
                    break
                elif (i+1==len(temp1)):
                    keys.append(value)
                    page.append(page_id_value)
                    slot.append(slot_id_value)
                    break
        else:
            keys.append(value)
            page.append(page_id_value)
            slot.append(slot_id_value)
        
        return keys, page, slot

    def insert_in_parent(self, left_node: bytes, key, right_node: bytes):
        leaft_page_id, leaft_is_leaf, leaft_num_keys, leaft_parent_id, leaft_next_leaf=self._read_header_pagina(left_node)
        right_page_id, right_is_leaf, right_num_keys, right_parent_id, right_next_leaf=self._read_header_pagina(right_node)
        
        if leaft_page_id==self.raiz:
            print("Creando nueva raíz")
            new_raiz_id=self.siguiente_pagina_libre
            self.siguiente_pagina_libre+=1

            llaves=[key]
            pointers=[leaft_page_id, right_page_id]
            self.raiz=new_raiz_id
            leaft_parent_id=new_raiz_id
            right_parent_id=new_raiz_id

            if leaft_is_leaf==1:
                k,p,s,_,nl,pid=self._unpack_leaf(left_node)
                left_node=self._pack_leaf(k, p, s, leaft_parent_id, nl, pid)
                k,p,s,_,nl,pid=self._unpack_leaf(right_node)
                right_node=self._pack_leaf(k, p, s, right_parent_id, nl, pid)
            else:
                k,ptrs,_,pid=self._unpack_internal(left_node)
                left_node=self._pack_internal(k,ptrs, leaft_parent_id, pid)
                k,ptrs,_,pid=self._unpack_internal(right_node)
                right_node=self._pack_internal(k,ptrs, right_parent_id, pid)

            #porque sabemos que todas las hojas estan al mismo nivel
            self.buffer.write_page(leaft_page_id, left_node)
            self.buffer.write_page(right_page_id, right_node)

            self.buffer.write_page(new_raiz_id, self._pack_internal(llaves, pointers, -1, new_raiz_id))
            
            self._write_metadata()

            return
        
        raw_padre=self.buffer.read_page(leaft_parent_id)
        keys_padre, pointers_padre, parent_id_padre, page_id_padre=self._unpack_internal(raw_padre)

        for i in range(len(pointers_padre)):
            if (pointers_padre[i]==leaft_page_id):
                pointers_padre.insert(i+1, right_page_id)
                keys_padre.insert(i, key)

                if len(keys_padre)>self.m:
                    new_padre_id=self.siguiente_pagina_libre
                    self.siguiente_pagina_libre+=1
                    self._write_metadata()

                    mid = len(keys_padre) // 2
                    
                    new_parent_llaves=keys_padre[mid+1:]
                    new_parent_pointers=pointers_padre[mid+1:]

                    value_a_subir=keys_padre[mid]
                    keys_padre=keys_padre[:mid]
                    pointers_padre=pointers_padre[:mid+1]

                    for j in (new_parent_pointers):
                        raw_pagina=self.buffer.read_page(j)
                        page_id, is_leaf, num_keys, parent_id, next_leaf=self._read_header_pagina(raw_pagina)
                        new_parent_id=new_padre_id
                        if is_leaf==1:
                            keys,page,slot,parent_id,next_leaf,page_id=self._unpack_leaf(raw_pagina)
                            pagina_actualizada=self._pack_leaf(keys, page, slot, new_parent_id, next_leaf, page_id)
                            self.buffer.write_page(page_id, pagina_actualizada)
                        else:
                            keys,pointers,parent_id,page_id=self._unpack_internal(raw_pagina)
                            pagina_actualizada=self._pack_internal(keys, pointers, new_parent_id, page_id)
                            self.buffer.write_page(page_id, pagina_actualizada)
                    raw_new_padre=self._pack_internal(new_parent_llaves, new_parent_pointers, parent_id_padre, new_padre_id)
                    self.buffer.write_page(new_padre_id, raw_new_padre)
                    raw_padre=self._pack_internal(keys_padre, pointers_padre, parent_id_padre, page_id_padre)
                    self.buffer.write_page(page_id_padre, raw_padre)
                    self.insert_in_parent(raw_padre, value_a_subir, raw_new_padre)
                else:
                    raw_padre=self._pack_internal(keys_padre, pointers_padre, parent_id_padre, page_id_padre)
                    self.buffer.write_page(page_id_padre, raw_padre)
                return

    def add(self, key, page_id_value: int, slot_id_value: int):
        key = self._normalize_key(key)
        old_node=self.search_leaf(key)
        keys, page, slot=self.insert_leaf(old_node, page_id_value, slot_id_value, key)
        page_id, is_leaf, num_keys, parent_id, next_leaf=struct.unpack_from('<iiiii',old_node,0)

        if (len(keys)>self.max_leaf_keys):

            mid=len(keys)//2

            right_keys=keys[mid:]
            right_page=page[mid:]
            right_slot=slot[mid:]

            left_keys=keys[:mid]
            left_page=page[:mid]
            left_slot=slot[:mid]

            nueva_hoja_id = self.siguiente_pagina_libre
            self.siguiente_pagina_libre += 1
            self._write_metadata()

            right_next_leaf=next_leaf
            left_next_leaf=nueva_hoja_id

            raw_left=self._pack_leaf(left_keys, left_page, left_slot, parent_id, left_next_leaf, page_id)
            raw_right=self._pack_leaf(right_keys, right_page, right_slot, parent_id, right_next_leaf, nueva_hoja_id)

            self.buffer.write_page(nueva_hoja_id, raw_right)
            self.buffer.write_page(page_id, raw_left)

            self.insert_in_parent(raw_left,right_keys[0],raw_right)
        else:
            updated_leaf=self._pack_leaf(keys, page, slot, parent_id, next_leaf, page_id)
            self.buffer.write_page(page_id, updated_leaf)

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
            pid, is_leaf, num_keys, parent_id, next_leaf = self._read_header_pagina(raw)
            if is_leaf == 1:
                keys, pages, slots, _, nl, _ = self._unpack_leaf(raw)
                keys_disp = [self._key_display(k) for k in keys]
                rids = list(zip(pages, slots))
                next_disp = f"Pág {nl}" if nl != -1 else "Fin"
                print(f"    [Hoja  pid={pid} parent={parent_id} next={next_disp}]  keys={keys_disp}  RIDs={rids}")
            else:
                keys, ptrs, _, _ = self._unpack_internal(raw)
                keys_disp = [self._key_display(k) for k in keys]
                print(f"    [Inter pid={pid} parent={parent_id}]                    keys={keys_disp}  ptrs={ptrs}")
                for ptr in ptrs:
                    cola.append((ptr, nivel + 1))
        print("--- FIN ÁRBOL ---\n")

    def _min_leaf_keys(self):
        return (self.max_leaf_keys + 1) // 2

    def _min_internal_pointers(self):
        return (self.m + 2) // 2

    def _set_parent(self, node_id, new_parent_id):
        raw = self.buffer.read_page(node_id)
        _, is_leaf, _, _, _ = self._read_header_pagina(raw)
        if is_leaf == 1:
            k, p, s, _, nl, pid = self._unpack_leaf(raw)
            raw = self._pack_leaf(k, p, s, new_parent_id, nl, pid)
        else:
            k, ptrs, _, pid = self._unpack_internal(raw)
            raw = self._pack_internal(k, ptrs, new_parent_id, pid)
        self.buffer.write_page(node_id, raw)

    def remove(self, key) -> dict:
        time_start = time.time()
        key = self._normalize_key(key)

        raw = self.search_leaf(key)
        keys, _, _, _, next_leaf, leaf_id = self._unpack_leaf(raw)

        while key not in keys and next_leaf != -1 and (not keys or keys[-1] < key):
            raw = self.buffer.read_page(next_leaf)
            keys, _, _, _, next_leaf, leaf_id = self._unpack_leaf(raw)

        if key not in keys:
            return self._format_result(False, self.buffer.get_io_cost(), time.time() - time_start)

        self._delete_entry(leaf_id, key, pointer=None)
        return self._format_result(True, self.buffer.get_io_cost(), time.time() - time_start)

    def _delete_entry(self, node_id, key, pointer):
        raw = self.buffer.read_page(node_id)
        _, is_leaf, _, parent_id, _ = self._read_header_pagina(raw)

        if is_leaf == 1:
            ks, ps, ss, _, nl, _ = self._unpack_leaf(raw)
            i = ks.index(key)
            ks.pop(i); ps.pop(i); ss.pop(i)
            raw = self._pack_leaf(ks, ps, ss, parent_id, nl, node_id)
            num_entries = len(ks)
        else:
            ks, ptrs, _, _ = self._unpack_internal(raw)
            idx_ptr = ptrs.index(pointer)
            if idx_ptr == 0:
                ks.pop(0)
            else:
                ks.pop(idx_ptr - 1)
            ptrs.pop(idx_ptr)
            raw = self._pack_internal(ks, ptrs, parent_id, node_id)
            num_entries = len(ptrs)
        self.buffer.write_page(node_id, raw)

        if node_id == self.raiz:
            if is_leaf == 0 and num_entries == 1:
                nueva_raiz = ptrs[0]
                self._set_parent(nueva_raiz, -1)
                self.raiz = nueva_raiz
                self._write_metadata()
            return

        if is_leaf == 1 and num_entries >= self._min_leaf_keys():
            return
        if is_leaf == 0 and num_entries >= self._min_internal_pointers():
            return

        raw_padre = self.buffer.read_page(parent_id)
        ks_p, ptrs_p, abuelo_id, _ = self._unpack_internal(raw_padre)
        idx = ptrs_p.index(node_id)
        izq_id = ptrs_p[idx - 1] if idx > 0 else None
        der_id = ptrs_p[idx + 1] if idx + 1 < len(ptrs_p) else None
        minimo = self._min_leaf_keys() if is_leaf == 1 else self._min_internal_pointers()

        if izq_id is not None:
            raw_izq = self.buffer.read_page(izq_id)
            if is_leaf == 1:
                ks_h, ps_h, ss_h, _, nl_h, _ = self._unpack_leaf(raw_izq)
                puede_prestar = len(ks_h) > minimo
            else:
                ks_h, ptrs_h, _, _ = self._unpack_internal(raw_izq)
                puede_prestar = len(ptrs_h) > minimo

            if puede_prestar:
                sep_idx = idx - 1
                if is_leaf == 1:
                    ks_n, ps_n, ss_n, _, nl_n, _ = self._unpack_leaf(raw)
                    ks_n.insert(0, ks_h.pop()); ps_n.insert(0, ps_h.pop()); ss_n.insert(0, ss_h.pop())
                    ks_p[sep_idx] = ks_n[0]
                    self.buffer.write_page(izq_id, self._pack_leaf(ks_h, ps_h, ss_h, parent_id, nl_h, izq_id))
                    self.buffer.write_page(node_id, self._pack_leaf(ks_n, ps_n, ss_n, parent_id, nl_n, node_id))
                else:
                    ks_n, ptrs_n, _, _ = self._unpack_internal(raw)
                    ptr_movido = ptrs_h.pop()
                    llave_subida = ks_h.pop()
                    ks_n.insert(0, ks_p[sep_idx])
                    ptrs_n.insert(0, ptr_movido)
                    self._set_parent(ptr_movido, node_id)
                    ks_p[sep_idx] = llave_subida
                    self.buffer.write_page(izq_id, self._pack_internal(ks_h, ptrs_h, parent_id, izq_id))
                    self.buffer.write_page(node_id, self._pack_internal(ks_n, ptrs_n, parent_id, node_id))
                self.buffer.write_page(parent_id, self._pack_internal(ks_p, ptrs_p, abuelo_id, parent_id))
                return
            
        if der_id is not None:
            raw_der = self.buffer.read_page(der_id)
            if is_leaf == 1:
                ks_h, ps_h, ss_h, _, nl_h, _ = self._unpack_leaf(raw_der)
                puede_prestar = len(ks_h) > minimo
            else:
                ks_h, ptrs_h, _, _ = self._unpack_internal(raw_der)
                puede_prestar = len(ptrs_h) > minimo

            if puede_prestar:
                sep_idx = idx
                if is_leaf == 1:
                    ks_n, ps_n, ss_n, _, nl_n, _ = self._unpack_leaf(raw)
                    ks_n.append(ks_h.pop(0)); ps_n.append(ps_h.pop(0)); ss_n.append(ss_h.pop(0))

                    ks_p[sep_idx] = ks_h[0]
                    self.buffer.write_page(der_id, self._pack_leaf(ks_h, ps_h, ss_h, parent_id, nl_h, der_id))
                    self.buffer.write_page(node_id, self._pack_leaf(ks_n, ps_n, ss_n, parent_id, nl_n, node_id))
                else:
                    ks_n, ptrs_n, _, _ = self._unpack_internal(raw)

                    ptr_movido = ptrs_h.pop(0)
                    llave_subida = ks_h.pop(0)
                    ks_n.append(ks_p[sep_idx])
                    ptrs_n.append(ptr_movido)
                    self._set_parent(ptr_movido, node_id)
                    ks_p[sep_idx] = llave_subida
                    self.buffer.write_page(der_id, self._pack_internal(ks_h, ptrs_h, parent_id, der_id))
                    self.buffer.write_page(node_id, self._pack_internal(ks_n, ptrs_n, parent_id, node_id))
                self.buffer.write_page(parent_id, self._pack_internal(ks_p, ptrs_p, abuelo_id, parent_id))
                return

        if izq_id is not None:
            sep_idx = idx - 1
            K_sep = ks_p[sep_idx]
            raw_izq = self.buffer.read_page(izq_id)
            if is_leaf == 1:
                ks_izq, ps_izq, ss_izq, _, _, _ = self._unpack_leaf(raw_izq)
                ks_n, ps_n, ss_n, _, nl_n, _ = self._unpack_leaf(raw)

                izq_nuevo = self._pack_leaf(ks_izq + ks_n, ps_izq + ps_n, ss_izq + ss_n,
                                            parent_id, nl_n, izq_id)
            else:
                ks_izq, ptrs_izq, _, _ = self._unpack_internal(raw_izq)
                ks_n, ptrs_n, _, _ = self._unpack_internal(raw)

                izq_nuevo = self._pack_internal(ks_izq + [K_sep] + ks_n, ptrs_izq + ptrs_n,
                                                parent_id, izq_id)

                for hijo_id in ptrs_n:
                    self._set_parent(hijo_id, izq_id)
            self.buffer.write_page(izq_id, izq_nuevo)

            self._delete_entry(parent_id, K_sep, node_id)
        else:
            sep_idx = idx
            K_sep = ks_p[sep_idx]
            raw_der = self.buffer.read_page(der_id)
            if is_leaf == 1:
                ks_n, ps_n, ss_n, _, _, _ = self._unpack_leaf(raw)
                ks_d, ps_d, ss_d, _, nl_d, _ = self._unpack_leaf(raw_der)
                n_nuevo = self._pack_leaf(ks_n + ks_d, ps_n + ps_d, ss_n + ss_d,
                                          parent_id, nl_d, node_id)
            else:
                ks_n, ptrs_n, _, _ = self._unpack_internal(raw)
                ks_d, ptrs_d, _, _ = self._unpack_internal(raw_der)
                n_nuevo = self._pack_internal(ks_n + [K_sep] + ks_d, ptrs_n + ptrs_d,
                                              parent_id, node_id)
                for hijo_id in ptrs_d:
                    self._set_parent(hijo_id, node_id)
            self.buffer.write_page(node_id, n_nuevo)
            self._delete_entry(parent_id, K_sep, der_id)

    def range_search(self, begin_key, end_key) -> dict:
        time_start = time.time()
        begin_key = self._normalize_key(begin_key)
        end_key = self._normalize_key(end_key)

        if begin_key > end_key:
            return self._format_result([], self.buffer.get_io_cost(), time.time() - time_start)

        raw = self.search_leaf(begin_key)
        results = []

        while True:
            keys, page, slot, _, next_leaf, _ = self._unpack_leaf(raw)
            for i in range(len(keys)):
                if keys[i] > end_key:
                    return self._format_result(results, self.buffer.get_io_cost(), time.time() - time_start)
                if keys[i] >= begin_key:
                    results.append((page[i], slot[i]))

            if next_leaf == -1:
                break
            if not keys or keys[-1] <= end_key:
                raw = self.buffer.read_page(next_leaf)
                continue
            break

        return self._format_result(results, self.buffer.get_io_cost(), time.time() - time_start)
