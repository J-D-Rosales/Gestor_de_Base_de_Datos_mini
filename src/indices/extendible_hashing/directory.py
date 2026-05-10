import struct
import os

class Directory:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.global_depth = 1
        # Array de Page IDs. El índice del array es el sufijo de bits.
        # Inicialmente con global_depth=1, tenemos 2 entradas (0 y 1).
        # Por defecto, la entrada 0 apunta a la página 0, y la 1 a la página 1.
        self.bucket_pointers = [0, 1] 
        
        self._load_or_init()

    def get_page_id(self, hash_suffix: int) -> int:
        """Dado el sufijo del hash extraído, retorna el ID de la página en disco."""
        # Aseguramos que el sufijo no exceda el tamaño del directorio
        index = hash_suffix & ((1 << self.global_depth) - 1)
        return self.bucket_pointers[index]

    def double(self):
        """
        Duplica el tamaño del directorio.
        En Hash Extensible (usando LSB - Least Significant Bits), duplicar
        significa que el nuevo directorio copia exactamente los punteros actuales
        y los anexa al final.
        """
        self.global_depth += 1
        # Python hace esto súper fácil: duplicamos la lista.
        # Ejemplo: Si era [0, 1] (apuntando a pag 0 y pag 1), 
        # ahora será [0, 1, 0, 1].
        self.bucket_pointers.extend(list(self.bucket_pointers))
        self.save()

    def update_pointers(self, hash_suffix: int, local_depth: int, new_page_id_0: int, new_page_id_1: int):
        """
        Después de un split de un bucket, actualiza todos los punteros del 
        directorio que apuntaban al bucket viejo para que ahora apunten 
        a los dos buckets nuevos, basándose en el nuevo bit significativo.
        """
        # Cuántas entradas en el directorio apuntan a este bucket?
        # Fórmula: 2^(global_depth - local_depth)
        num_pointers = 1 << (self.global_depth - local_depth)
        
        # El local_depth ya fue incrementado en el bucket antes de llamar a esto.
        # El bit que acabamos de agregar a la "máscara" del local_depth
        new_bit_mask = 1 << (local_depth - 1)
        
        # Recorremos todos los punteros que comparten el sufijo base
        base_mask = (1 << (local_depth - 1)) - 1
        base_suffix = hash_suffix & base_mask

        for i in range(1 << self.global_depth):
            if (i & base_mask) == base_suffix:
                # Si el nuevo bit significativo es 0, apunta al primer bucket nuevo
                if (i & new_bit_mask) == 0:
                    self.bucket_pointers[i] = new_page_id_0
                # Si el nuevo bit significativo es 1, apunta al segundo bucket nuevo
                else:
                    self.bucket_pointers[i] = new_page_id_1
                    
        self.save()

    def save(self):
        """
        Persiste el directorio a disco (en un archivo .dir o similar).
        Un índice de base de datos DEBE sobrevivir a reinicios.
        """
        with open(self.filepath, 'wb') as f:
            # Guardamos la profundidad global (4 bytes) y el número de entradas (4 bytes)
            num_entries = len(self.bucket_pointers)
            f.write(struct.pack('ii', self.global_depth, num_entries))
            # Guardamos todos los punteros (4 bytes cada uno)
            f.write(struct.pack(f'{num_entries}i', *self.bucket_pointers))

    def _load_or_init(self):
        """Carga el directorio desde el disco si existe."""
        if os.path.exists(self.filepath) and os.path.getsize(self.filepath) > 0:
            with open(self.filepath, 'rb') as f:
                header = f.read(8)
                self.global_depth, num_entries = struct.unpack('ii', header)
                data = f.read(num_entries * 4)
                self.bucket_pointers = list(struct.unpack(f'{num_entries}i', data))