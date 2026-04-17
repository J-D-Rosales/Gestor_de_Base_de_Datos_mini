import time

class SQLParser:
    def __init__(self):
        self.catalog = {}
        self.factory = {} # Aquí importarás las clases de tus compañeros luego

    def execute_query(self, query: str) -> dict:
        """Punto de entrada de las consultas. Deberás mejorar esto con PLY o tu propio autómata."""
        start_time = time.time()
        tokens = query.strip().split()
        
        if not tokens:
            return {"error": "Consulta vacía"}

        command = tokens[0].upper()
        
        try:
            # Ejemplo de mock para que pruebes que la conexión funciona
            if command == "CREATE":
                table_name = tokens[2]
                # self.catalog[table_name] = self.factory[tokens[4]](table_name)
                return {"status": f"MOCK: Tabla {table_name} creada."}
            else:
                return {"error": "Comando SQL no reconocido o no implementado aún."}
                
        except Exception as e:
            return {"error": str(e)}