from base_index import BaseIndex
import struct
import os
from buffer_manager import BufferManager
import time

class BPlusTreeIndex(BaseIndex):
    PAGE_SIZE= 4096
    HEADER_SIZE= 12 #is_leaf(4 bytes) + num_keys(4 bytes) + parent_id(4 bytes) 
    POINTER_SIZE= 4
    RID_SIZE= 8 #page_id(4 bytes) + slot_id(4 bytes)

    def __init__ (self, table_name, index_name, idx_key, idx_size):
        super().__init__(table_name)
        self.index_name = index_name
        self.idx_key = idx_key
        self.filepath = f"data/{index_name}.bin"
        self.buffer=BufferManager(self.filepath, self.PAGE_SIZE)

        if idx_key.upper()=="INT" or idx_key.upper()=="FLOAT":
            self.key_format ='<i'
            self.key_size = 4
        elif idx_key.upper()=="STR":
            self.key_format = f'<{idx_size}s'
            self.key_size= idx_size
        
        self.m=(self.PAGE_SIZE - self.HEADER_SIZE) // (self.key_size + self.POINTER_SIZE) #orden del árbol
        
        self.max_leaf_keys=(self.PAGE_SIZE - self.HEADER_SIZE) // (self.key_size + self.RID_SIZE) #máximo número de claves en una hoja
        if not os.path.exists(self.filepath):
            self._initialize_tree()
    
    def _initialize_tree(self):
        #insertar todos los registros en el arbol
        pass

    def search_leaf(self, value) -> bytes:
        current_node=self.buffer.read_page(self.raiz)
        offset=0
        is_leaf, num_keys, parent_id=struct.unpack_from('<iii',current_node,offset)
        offset+=self.HEADER_SIZE
        
        while is_leaf==0:
            formato_pares="<i"+("ii")*num_keys
            datos_extraidos=struct.unpack_from(formato_pares,current_node,offset)
            keys=datos_extraidos[1::2] #sabemos que Nkeys=Npointers-1
            pointers=datos_extraidos[0::2]

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

        return current_node
    
    def search(self, key) -> dict:
        time_start = time.time()
        l=self.search_leaf(key)
        offset=0
        is_leaf, num_keys, parent_id=struct.unpack_from('<iii',l,offset)
        offset+=self.HEADER_SIZE

        formato_pares="<"+("iii")*num_keys #llave, page_id, slot_id
        datos_extraidos=struct.unpack_from(formato_pares,l,offset)
        keys=datos_extraidos[0::4]
        page=datos_extraidos[1::4]
        slot=datos_extraidos[2::4]

        for i in range(len(keys)):
            if key==keys[i]:
                time_end = time.time()
                return self._format_result((page[i],slot[i]), self.buffer.get_io_cost(), time_end - time_start)
        
        return self._format_result(None, self.buffer.get_io_cost(), time_end - time_start)
    
    def insert_leaf(self, nodo_hoja: bytes, page_id_value: int, slot_id_value: int, value):
        offset=0
        is_leaf, num_keys, parent_id=struct.unpack_from('<iii',nodo_hoja,offset)
        offset+=self.HEADER_SIZE

        formato_pares="<"+("iii")*num_keys #llave, page_id, slot_id
        datos_extraidos=struct.unpack_from(formato_pares,nodo_hoja,offset)
        keys=datos_extraidos[0::4]
        page=datos_extraidos[1::4]
        slot=datos_extraidos[2::4]

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

    def add(self, key, record) -> dict:
        old_node=self.search_leaf(key)
        keys, page, slot=self.insert_leaf(old_node, record[0], record[1], key)