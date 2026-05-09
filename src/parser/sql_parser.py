from pathlib import Path
import sys

import ply.yacc as yacc

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    src_dir = project_root / "src"
    indices_dir = src_dir / "indices"
    # Los módulos de indices/ y executor.py usan imports bare estilo
    # `from base_index import ...`, así que necesitan src/ y src/indices/
    # en sys.path (mismo truco que executor.py).
    for _p in (str(project_root), str(src_dir), str(indices_dir)):
        if _p not in sys.path:
            sys.path.insert(0, _p)

    from src.parser.sql_lexer import tokens
    from src.executor import Executor
else:
    from .sql_lexer import tokens
    from ..executor import Executor

class SQLParser:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parents[2]
        self.tokens = tokens  # Necesario para que yacc sepa qué tokens usar
        self.parser = yacc.yacc(module=self)
        # Backend de ejecución (catálogo, índices, sequential file). Lo mantenemos
        # como singleton para que el estado persista entre llamadas a run().
        self._executor = Executor()

    def _resolve_path(self, raw_path):
        candidate = Path(raw_path)
        if candidate.is_absolute() and candidate.exists():
            return candidate

        search_roots = [Path.cwd(), self.project_root, self.project_root / "src", self.project_root / "src" / "data", self.project_root / "data"]
        for root in search_roots:
            resolved = (root / raw_path).resolve()
            if resolved.exists():
                return resolved

        return (self.project_root / raw_path).resolve()

    # --- API pública para el frontend -----------------------------------
    def execute(self, query):
        """Devuelve solo el AST (lista de dicts/strings). No ejecuta nada."""
        return self.parser.parse(query)

    # Alias retrocompatible.
    def execute_query(self, query):
        return self.execute(query)

    def run(self, query):
        """Parsea y ejecuta. Delega cada AST al Executor (catálogo + índices
        + sequential file). Devuelve una lista de resultados, uno por sentencia,
        con la misma forma para todas las operaciones (status/op/...)."""
        asts = self.parser.parse(query)
        if asts is None:
            return [{"status": "error", "msg": "Sintaxis inválida o consulta vacía"}]
        return [self._executor.execute(ast) for ast in asts]

    # --- REGLAS DE LA GRAMÁTICA ---
    #==================
    # Sentencias
    #==================    
    # =========================================================
    # DEPARTAMENTO PRINCIPAL: RECOLECTOR DE SENTENCIAS
    # (Debe ser la primera regla de tu parser)
    # =========================================================

    def p_lista_sentencias_varias(self, p):
        """
        lista_sentencias : lista_sentencias sentencia SEMICOLON
        """
        # Si ya traíamos una lista de tickets (p[1]), le agregamos el nuevo ticket (p[2])
        p[0] = p[1] + [p[2]]

    def p_lista_sentencias_una(self, p):
        """
        lista_sentencias : sentencia SEMICOLON
        """
        # Empezamos la recolección metiendo el primer ticket (p[1]) en una lista
        p[0] = [p[1]]
    def p_sentencia_create_file_archivos(self, p):
        """
        sentencia : CREATE TABLE ID LPAREN lista_columnas RPAREN FROM FILE STRING
        """
        # Resolvemos la ruta a absoluta para que el executor no la re-resuelva
        # contra su propio data_dir.
        csv_path = str(self._resolve_path(p[9]))
        p[0] = {
            "tipo_operacion": "CREATE_TABLE",
            "tabla": p[3].lower(),
            "columnas": p[5],
            "csv_path": csv_path,
        }

    def p_sentencia_create_file_sin_archivos(self, p):
        """
        sentencia : CREATE TABLE ID LPAREN lista_columnas RPAREN
        """
        p[0] = {
            "tipo_operacion": "CREATE_TABLE",
            "tabla": p[3].lower(),
            "columnas": p[5],
        }

    # 1. SELECT * de Búsqueda Puntual (Point Query)
    def p_sentencia_select_asterisco_igualdad(self, p):
        """
        sentencia : SELECT ASTERISK FROM ID WHERE ID EQUALS valor
        """
        # SELECT * FROM tabla WHERE col = 100
        p[0] = {
            "tipo_operacion": "SELECT_POINT",
            "tabla": p[4].lower(),
            "columna_select": "*",
            "condicion": {
                "columna_where": p[6],
                "operador": "=",
                "valor": p[8]
            }
        }

    # 2. SELECT * de Rango (Range Query)
    def p_sentencia_select_asterisco_rango(self, p):
        """
        sentencia : SELECT ASTERISK FROM ID WHERE ID BETWEEN valor AND valor
        """
        # SELECT * FROM tabla WHERE col BETWEEN 10 AND 50
        p[0] = {
            "tipo_operacion": "SELECT_RANGE",
            "tabla": p[4].lower(),
            "columna_select": "*",
            "condicion": {
                "columna_where": p[6],
                "valor_inicio": p[8],
                "valor_fin": p[10]
            }
        }
    # 3. SELECT Espacial con Radio (Range Search)
    def p_sentencia_select_espacial_radio(self, p):
        """
        sentencia : SELECT ASTERISK FROM ID WHERE ID IN LPAREN POINT LPAREN valor COMMA valor RPAREN COMMA RADIUS valor RPAREN
        """
        # p:  1       2     3   4   5   6   7   8      9    10    11    12   13    14     15     16     17    18
        # Ej: SELECT  * FROM ubi WHERE col IN  (  POINT   (    x      ,    y      )      ,    RADIUS  10    )
        
        p[0] = {
            "tipo_operacion": "SELECT_SPATIAL_RADIUS",
            "tabla": p[4].lower(),
            "columna_select": "*",
            "condicion": {
                "columna_where": p[6],
                "coordenada_x": p[11],
                "coordenada_y": p[13],
                "radio": p[17]
            }
        }

    # 4. SELECT Espacial por Vecinos Cercanos (kNN)
    def p_sentencia_select_espacial_knn(self, p):
        """
        sentencia : SELECT ASTERISK FROM ID WHERE ID IN LPAREN POINT LPAREN valor COMMA valor RPAREN COMMA K valor RPAREN
        """
        # p:  1       2     3   4   5   6   7   8      9    10    11    12   13    14     15   16  17   18
        # Ej: SELECT  * FROM ubi WHERE col IN  (  POINT   (    x      ,    y      )      ,   K   5    )
        
        p[0] = {
            "tipo_operacion": "SELECT_SPATIAL_KNN",
            "tabla": p[4].lower(),
            "columna_select": "*",
            "condicion": {
                "columna_where": p[6],
                "coordenada_x": p[11],
                "coordenada_y": p[13],
                "k_vecinos": p[17]
            }
        }
    # 5. INSERT INTO
    def p_sentencia_insert(self, p):
        """
        sentencia : INSERT INTO ID VALUES LPAREN lista_valores RPAREN
        """
        # INSERT INTO tabla VALUES ( 1 , 'Lima' , 4.5 )
        p[0] = {
            "tipo_operacion": "INSERT",
            "tabla": p[3].lower(),
            "valores_a_insertar": p[6] # p[6] es la lista de Python que te devuelve tu sub-regla
        }

    # Para recolectar los valores del INSERT (Igual a como hiciste con las columnas):
    def p_lista_valores_varios(self, p):
        """
        lista_valores : lista_valores COMMA valor
        """
        p[0] = p[1] + [p[3]]

    def p_lista_valores_uno(self, p):
        """
        lista_valores : valor
        """
        p[0] = [p[1]]

    # 6. DELETE FROM
    def p_sentencia_delete(self, p):
        """
        sentencia : DELETE FROM ID WHERE ID EQUALS valor
        """
        # DELETE FROM tabla WHERE id = 10
        p[0] = {
            "tipo_operacion": "DELETE",
            "tabla": p[3].lower(),
            "condicion": {
                "columna_where": p[5],
                "operador": "=",
                "valor": p[7]
            }
        }   
    # === DEPARTAMENTO DE VALORES (El comodín) ===
    def p_valor(self, p):
        """
        valor : STRING
              | NUMBER  
        """
        # (El símbolo | significa "O". Acepta un STRING o un NUMBER)
        p[0] = p[1]

    # =========================================================
    # EL DEPARTAMENTO QUE ARMA LOS DICCIONARIOS (Los empaquetadores)
    # =========================================================
    def p_columna_con_indice_compuesto(self, p):
        """
        columna : ID tipo_dato INDEX tecnica_indice LPAREN ID COMMA ID RPAREN
        """
        p[0] = {
            "nombre": p[1], 
            "tipo": p[2], 
            "indice": p[4], 
            "rtree_cols": [p[6], p[8]] # Guardamos [longitud, latitud]
        }

    def p_columna_con_indice(self, p):
        """
        columna : ID tipo_dato INDEX tecnica_indice
        """
        p[0] = {"nombre": p[1], "tipo": p[2], "indice": p[4], "rtree_cols": None}

    # 3. Columna sin índice
    def p_columna_sin_indice(self, p):
        """
        columna : ID tipo_dato
        """
        p[0] = {"nombre": p[1], "tipo": p[2], "indice": None, "rtree_cols": None}

    # =========================================================
    # EL DEPARTAMENTO QUE ARMA LA LISTA (Los recolectores)
    # =========================================================

    def p_lista_columnas_varias(self, p):
        """
        lista_columnas : lista_columnas COMMA columna
        """
        # p[1] es la lista que ya traíamos.
        # p[3] es el DICCIONARIO completo que nos entregó el departamento "columna"
        p[0] = p[1] + [p[3]] 

    def p_lista_columnas_una(self, p):
        """
        lista_columnas : columna
        """
        # p[1] es el DICCIONARIO completo que nos entregó el departamento "columna"
        # Lo metemos dentro de una lista [ ] para empezar la recolección
        p[0] = [p[1]]

    # === TIPOS PERMITIDOS ===
    def p_tipo_dato(self, p):
        """
        tipo_dato : INT
                  | FLOAT
                  | VARCHAR
                  | STR
                  | POINT
        """
        p[0] = p[1].upper()

    # === DEPARTAMENTO DE TÉCNICAS PERMITIDAS ===
    def p_tecnica_indice(self, p):
        """
        tecnica_indice : BTREE
                       | RTREE
                       | HASH
                       | SEQUENTIAL
        """
        p[0] = p[1].upper()
    def p_error(self, p):
        print(f"Error de sintaxis en: {p.value if p else 'Fin de archivo'}")

# Pruebas rápidas
if __name__ == "__main__":
    db = SQLParser()
    ruta = "src/data/airbnb_1000.csv"
    sql = (
        f"CREATE TABLE airbnb (id INT INDEX BTREE, name VARCHAR, "
        f"city VARCHAR INDEX BTREE, lat FLOAT, long FLOAT, price FLOAT, "
        f"room_type VARCHAR, cap INT) FROM FILE '{ruta}';"
        "SELECT * FROM airbnb WHERE id = 2577;"
        "SELECT * FROM airbnb WHERE city = 'Paris';"
    )
    print("--- INICIANDO PARSER ---")
    for i, res in enumerate(db.run(sql)):
        print(f"Instrucción {i+1}:", res)