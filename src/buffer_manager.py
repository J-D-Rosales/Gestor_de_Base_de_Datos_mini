# src/buffer_manager.py
import os

class BufferManager:
    def __init__(self, filepath, page_size=4096):
        self.filepath = filepath
        self.page_size = page_size
        self.reads = 0
        self.writes = 0

        # Si el archivo no existe en la carpeta data/, lo creamos vacío
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'wb') as f:
                pass

    def read_page(self, page_id: int) -> bytes:
        """Lee exactamente una página (4KB) del disco."""
        self.reads += 1
        with open(self.filepath, 'rb') as f:
            f.seek(page_id * self.page_size)
            data = f.read(self.page_size)
            # Si el archivo es más corto o la página está vacía, rellenamos con ceros
            return data.ljust(self.page_size, b'\0')

    def write_page(self, page_id: int, data: bytes):
        """Escribe exactamente una página (4KB) en el disco."""
        if len(data) > self.page_size:
            raise ValueError(f"CRÍTICO: La data ({len(data)} bytes) excede el tamaño de página de {self.page_size} bytes.")
        
        self.writes += 1
        # Asegurarnos de que mida exactamente 4096 bytes rellenando con bytes nulos
        padded_data = data.ljust(self.page_size, b'\0')
        
        with open(self.filepath, 'r+b') as f:
            f.seek(page_id * self.page_size)
            f.write(padded_data)

    def get_io_cost(self) -> int:
        """Retorna el total de accesos a disco (Lecturas + Escrituras)"""
        return self.reads + self.writes
    
    def reset_io_cost(self):
        """Reinicia el contador antes de cada nueva consulta SQL"""
        self.reads = 0
        self.writes = 0