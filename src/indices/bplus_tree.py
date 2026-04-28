from base_index import BaseIndex
import struct
import os
from buffer_manager import BufferManager
import time

class BPlusTreeIndex(BaseIndex):
    PAGE_SIZE= 4096
    HEADER_SIZE= 20 #page_id(4 bytes) + is_leaf(4 bytes) + num_keys(4 bytes) + parent_id(4 bytes) + next_leaf(4 bytes, solo para hojas)
    POINTER_SIZE= 4 #page_id de un nodo hijo(4 bytes)
    RID_SIZE= 8 #page_id(4 bytes) + slot_id(4 bytes)
    META_DATA_SIZE= 4 #page_root_id(4 bytes) + sigueinte_pagina_libre(4 bytes)

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
        
        self.m=(self.PAGE_SIZE - self.HEADER_SIZE) // (self.key_size + self.POINTER_SIZE) #orden del árbol
        
        self.max_leaf_keys=(self.PAGE_SIZE - self.HEADER_SIZE) // (self.key_size + self.RID_SIZE) #máximo número de claves en una hoja
        if not os.path.exists(self.filepath):
            self._initialize_tree()
        else:
            meta_data=self.buffer.read_page(0)
            self.raiz, self.siguiente_pagina_libre=struct.unpack_from('<ii',meta_data,0)
    
    def _initialize_tree(self):
        self.raiz=1
        self.siguiente_pagina_libre=2
        root_leaf=self.pack_leaf([],[],[],-1,-1,1)
        meta_data=bytearray(self.PAGE_SIZE)
        struct.pack_into('<ii', meta_data, 0, self.raiz, self.siguiente_pagina_libre)
        self.buffer.write_page(0, meta_data)
        self.buffer.write_page(1, root_leaf)
    
    def read_header_pagina(self,raw_page: bytes):
        page_id, is_leaf, num_keys, parent_id, next_leaf=struct.unpack_from('<iiiii',raw_page,0)
        return page_id, is_leaf, num_keys, parent_id, next_leaf

    def pack_leaf(self,keys,page,slot,parent_id,next_leaf,page_id) -> bytes:
        buffer=bytearray(self.PAGE_SIZE)

        offset=0    
        struct.pack_into('<iiiii', buffer, offset, page_id, 1, len(keys), parent_id, next_leaf)
        offset+=self.HEADER_SIZE

        formato_fila = f'<{self.key_format}ii'

        for i in range(len(keys)):
            llave_actual=keys[i]
            if self.idx_key.upper() == "STR":
                llave_bytes = str(llave_actual).encode('utf-8')
                llave_actual = llave_bytes

            struct.pack_into(formato_fila, buffer, offset, llave_actual, page[i], slot[i])
            offset+=self.key_size+self.RID_SIZE
        
        return buffer

    def unpack_leaf(self, raw_leaf: bytes):
        offset=0
        page_id, is_leaf, num_keys, parent_id, next_leaf=self.read_header_pagina(raw_leaf)
        offset+=self.HEADER_SIZE

        formato_pares="<"+(f"{self.key_format}ii")*num_keys #llave, page_id, slot_id
        datos_extraidos=struct.unpack_from(formato_pares,raw_leaf,offset)
        keys=list(datos_extraidos[0::3])
        page=list(datos_extraidos[1::3])
        slot=list(datos_extraidos[2::3])

        return keys, page, slot, parent_id, next_leaf, page_id
    
    def pack_internal(self, keys, pointers, parent_id, page_id) -> bytes:
        buffer=bytearray(self.PAGE_SIZE)
        offset=0
        struct.pack_into('<iiiii', buffer, offset, page_id, 0, len(keys), parent_id, -1)
        offset+=self.HEADER_SIZE

        formato_fila = f'<i{self.key_format}i' #puntero, llave, puntero
        for i in range(len(keys)):
            llave_actual = keys[i]
            if self.idx_key.upper() == "STR":
                llave_bytes = str(llave_actual).encode('utf-8')
                llave_actual = llave_bytes

            struct.pack_into(formato_fila, buffer, offset, pointers[i], llave_actual, pointers[i+1])
            offset+=self.POINTER_SIZE+self.key_size
        
        return buffer
    
    def unpack_internal(self, raw_internal: bytes):
        offset=0
        page_id, is_leaf, num_keys, parent_id, next_leaf=self.read_header_pagina(raw_internal)
        offset+=self.HEADER_SIZE

        formato_pares="<i"+(f"{self.key_format}i")*num_keys #puntero, llave, puntero
        datos_extraidos=struct.unpack_from(formato_pares,raw_internal,offset)
        pointers=datos_extraidos[0::2]
        keys=list(datos_extraidos[1::2])

        return keys, pointers, parent_id, page_id
    
    def search_leaf(self, value) -> bytes:
        current_node=self.buffer.read_page(self.raiz)
        offset=0
        page_id, is_leaf, num_keys, parent_id, next_leaf=self.read_header_pagina(current_node)
        offset+=self.HEADER_SIZE
        
        while is_leaf==0:
            keys,pointers, parent_id, page_id=self.unpack_internal(current_node)

            for i in range(len(keys)):
                if value==keys[i]:
                    current_node=self.buffer.read_page(pointers[i+1])
                    break
                elif value<keys[i]:
                    current_node=self.buffer.read_page(pointers[i])
                    break
                elif (i+1==len(keys)):
                    current_node=self.buffer.read_page(pointers[i+1])
                    break
            
            page_id, is_leaf, num_keys, parent_id, next_leaf=self.read_header_pagina(current_node)

        return current_node
    
    def search(self, key) -> dict:
        time_start = time.time()
        l=self.search_leaf(key)
        offset=0
        page_id, is_leaf, num_keys, parent_id, next_leaf=self.read_header_pagina(l)
        offset+=self.HEADER_SIZE

        formato_pares="<"+(f"{self.key_format}ii")*num_keys #llave, page_id, slot_id
        datos_extraidos=struct.unpack_from(formato_pares,l,offset)
        keys=datos_extraidos[0::3]
        page=datos_extraidos[1::3]
        slot=datos_extraidos[2::3]

        for i in range(len(keys)):
            if key==keys[i]:
                time_end = time.time()
                return self._format_result((page[i],slot[i]), self.buffer.get_io_cost(), time_end - time_start)
        
        time_end = time.time()
        return self._format_result(None, self.buffer.get_io_cost(), time_end - time_start)

    def insert_leaf(self, nodo_hoja: bytes, page_id_value: int, slot_id_value: int, value):

        keys,page,slot=self.unpack_leaf(nodo_hoja)

        if len(keys)!=0:
            temp1=keys
            for i in range(len(temp1)):
                if (value==temp1[i]):
                    keys.insert(i+1,value)
                    page.append(page_id_value)
                    slot.append(slot_id_value)
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
        leaft_page_id, leaft_is_leaf, leaft_num_keys, leaft_parent_id, leaft_next_leaf=self.read_header_pagina(left_node)
        right_page_id, right_is_leaf, right_num_keys, right_parent_id, right_next_leaf=self.read_header_pagina(right_node)
        
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
                left_node=self.pack_leaf(*self.unpack_leaf(left_node), leaft_page_id)
                right_node=self.pack_leaf(*self.unpack_leaf(right_node), right_page_id)
            else:
                left_node=self.pack_internal(*self.unpack_internal(left_node), leaft_parent_id, leaft_page_id)
                right_node=self.pack_internal(*self.unpack_internal(right_node), right_parent_id, right_page_id)

            #porque sabemos que todas las hojas estan al mismo nivel
            self.buffer.write_page(leaft_page_id, left_node)
            self.buffer.write_page(right_page_id, right_node)

            self.buffer.write_page(new_raiz_id, self.pack_internal(llaves, pointers, -1, new_raiz_id))
            
            return
        
        raw_padre=self.buffer.read_page(leaft_parent_id)
        keys_padre, pointers_padre, parent_id_padre, page_id_padre=self.unpack_internal(raw_padre)

        for i in range(len(pointers_padre)):
            if (pointers_padre[i]==leaft_page_id):
                pointers_padre.insert(i+1, right_page_id)
                keys_padre.insert(i, key)

                if len(keys_padre)>self.m:
                    new_padre_id=self.siguiente_pagina_libre
                    self.siguiente_pagina_libre+=1
                    mid = len(keys_padre) // 2
                    
                    new_parent_llaves=keys_padre[mid+1:]
                    new_parent_pointers=pointers_padre[mid+1:]

                    value_a_subir=keys_padre[mid]
                    if (mid==0):
                        keys_padre=keys_padre[:mid+1]
                    else:
                        keys_padre=keys_padre[:mid]
                    pointers_padre=pointers_padre[:mid+1]
                    for j in (pointers_padre):
                        raw_pagina=self.buffer.read_page(j)
                        page_id, is_leaf, num_keys, parent_id, next_leaf=self.read_header_pagina(raw_pagina)
                        new_parent_id=page_id_padre
                        if is_leaf==1:
                            keys,page,slot,parent_id,next_leaf,page_id=self.unpack_leaf(raw_pagina)
                            pagina_actualizada=self.pack_leaf(keys, page, slot, new_parent_id, next_leaf, page_id)
                            self.buffer.write_page(page_id, pagina_actualizada)
                        else:
                            keys,pointers,parent_id,page_id=self.unpack_internal(raw_pagina)
                            pagina_actualizada=self.pack_internal(keys, pointers, new_parent_id, page_id)
                            self.buffer.write_page(page_id, pagina_actualizada)

                    for j in (new_parent_pointers):
                        raw_pagina=self.buffer.read_page(j)
                        page_id, is_leaf, num_keys, parent_id, next_leaf=self.read_header_pagina(raw_pagina)
                        new_parent_id=new_padre_id
                        if is_leaf==1:
                            keys,page,slot,parent_id,next_leaf,page_id=self.unpack_leaf(raw_pagina)
                            pagina_actualizada=self.pack_leaf(keys, page, slot, new_parent_id, next_leaf, page_id)
                            self.buffer.write_page(page_id, pagina_actualizada)
                        else:
                            keys,pointers,parent_id,page_id=self.unpack_internal(raw_pagina)
                            pagina_actualizada=self.pack_internal(keys, pointers, new_parent_id, page_id)
                            self.buffer.write_page(page_id, pagina_actualizada)
                    raw_new_padre=self.pack_internal(new_parent_llaves, new_parent_pointers, parent_id_padre, new_padre_id)
                    self.buffer.write_page(new_padre_id, raw_new_padre)
                    raw_padre=self.pack_internal(keys_padre, pointers_padre, parent_id_padre, page_id_padre)
                    self.buffer.write_page(page_id_padre, raw_padre)
                    self.insert_in_parent(raw_padre, value_a_subir, raw_new_padre)


    def add(self, key, page_id_value: int, slot_id_value: int):
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

            right_next_leaf=next_leaf
            left_next_leaf=nueva_hoja_id

            raw_left=self.pack_leaf(left_keys, left_page, left_slot, parent_id, left_next_leaf, page_id)
            raw_right=self.pack_leaf(right_keys, right_page, right_slot, parent_id, right_next_leaf, nueva_hoja_id)

            self.buffer.write_page(nueva_hoja_id, raw_right)
            self.buffer.write_page(page_id, raw_left)

            self.insert_in_parent(raw_left,right_keys[0],raw_right)
        else:
            updated_leaf=self.pack_leaf(keys, page, slot, parent_id, next_leaf, page_id)
            self.buffer.write_page(page_id, updated_leaf)
