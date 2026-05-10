import struct

class Bucket:
    # Header: local_depth (int), record_count (int), overflow_page_id (int)
    HEADER_FORMAT = 'iii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, page_size: int, record_format: str, local_depth: int = 1):
        self.page_size = page_size
        self.record_format = record_format
        self.record_size = struct.calcsize(record_format)
        
        # Calculamos cuántos registros caben en la página físicamente
        self.max_records = (self.page_size - self.HEADER_SIZE) // self.record_size
        
        self.local_depth = local_depth
        self.records = []  # Almacenará tuplas (key, value)
        self.overflow_page = -1

    @classmethod
    def from_bytes(cls, data: bytes, page_size: int, record_format: str) -> "Bucket":
        # Desempaquetamos los primeros 12 bytes (el header)
        local_depth, count, overflow = struct.unpack_from(cls.HEADER_FORMAT, data, 0)
        
        # Instanciamos el bucket con los datos recuperados
        bucket = cls(page_size, record_format, local_depth)
        bucket.overflow_page = overflow
        
        # Leemos secuencialmente cada registro según el formato
        offset = cls.HEADER_SIZE
        for _ in range(count):
            record = struct.unpack_from(record_format, data, offset)
            bucket.records.append(record)
            offset += bucket.record_size
            
        return bucket

    def to_bytes(self) -> bytes:
        # Creamos un arreglo de bytes inicializado en ceros
        buffer = bytearray(self.page_size)
        
        # Empaquetamos el header al inicio del buffer
        struct.pack_into(self.HEADER_FORMAT, buffer, 0, 
                         self.local_depth, len(self.records), self.overflow_page)
                         
        # Iteramos y empaquetamos cada registro en su offset correspondiente
        offset = self.HEADER_SIZE
        for record in self.records:
            struct.pack_into(self.record_format, buffer, offset, *record)
            offset += self.record_size
            
        return bytes(buffer)

    def is_full(self) -> bool:
        # Verificación rápida de capacidad
        return len(self.records) >= self.max_records

    def insert(self, key, value) -> bool:
        # Retorna False si el Orquestador necesita hacer un Split
        if self.is_full():
            return False
            
        self.records.append((key, value))
        return True

    def remove(self, key) -> bool:
        # Busca la llave, la elimina y retorna True si hubo éxito
        for i, record in enumerate(self.records):
            if record[0] == key:
                del self.records[i]
                return True
        return False
        
    def clear(self):
        # Utilidad para vaciar el bucket durante el proceso de Split
        self.records = []