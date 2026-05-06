from pathlib import Path
import csv
import sys

import ply.yacc as yacc

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.parser.sql_lexer import tokens
    from src.indices.bplus_tree import BPlusTreeIndex
    from src.indices.extendible_hashing import ExtendibleHashing
    from src.indices.r_tree import RTree
else:
    from .sql_lexer import tokens
    from ..indices.bplus_tree import BPlusTreeIndex
    from ..indices.extendible_hashing import ExtendibleHashing
    from ..indices.r_tree import RTree

class SQLParser:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parents[2]
        self.data_dir = self.project_root / "data"
        self.catalog = {}
        self.indices = {}
        self.tokens = tokens # Necesario para que yacc sepa qué tokens usar
        self.parser = yacc.yacc(module=self)

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

    def _ensure_data_dir(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _index_name(self, table_name, column_name, suffix):
        return f"{table_name}_{column_name}_{suffix}"

    def _cast_value(self, raw_value, data_type):
        kind = (data_type or "").upper()
        if kind == "INT":
            return int(raw_value)
        if kind == "FLOAT":
            return float(raw_value)
        return raw_value

    def _load_indices_from_csv(self, table_name, columnas, csv_path):
        self._ensure_data_dir()
        self.indices.setdefault(table_name, {})

        csv_path = self._resolve_path(csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo CSV: {csv_path}")

        indexed_columns = [col for col in columnas if col.get("indice") in {"BTREE", "HASH", "RTREE"}]
        if not indexed_columns:
            return 0

        with open(csv_path, mode="r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return 0

            header_map = {name.lower(): idx for idx, name in enumerate(header)}
            row_number = 0

            for row in reader:
                row_number += 1
                for col in indexed_columns:
                    if col["indice"] == "BTREE":
                        idx = header_map.get(col["nombre"].lower())
                        if idx is None or idx >= len(row):
                            continue

                        index = self.indices[table_name].get(col["nombre"])
                        if index is None:
                            index = BPlusTreeIndex(
                                table_name,
                                self._index_name(table_name, col["nombre"], "btree"),
                                col["tipo"],
                                60 if col["tipo"].upper() in {"VARCHAR", "STR"} else 0,
                            )
                            self.indices[table_name][col["nombre"]] = index

                        value = self._cast_value(row[idx], col["tipo"])
                        index.add(value, row_number, 0)

                    elif col["indice"] == "HASH":
                        idx = header_map.get(col["nombre"].lower())
                        if idx is None or idx >= len(row):
                            continue

                        index = self.indices[table_name].get(col["nombre"])
                        if index is None:
                            index = ExtendibleHashing(table_name, self._index_name(table_name, col["nombre"], "hash"), col["tipo"], key_size=60)
                            self.indices[table_name][col["nombre"]] = index

                        value = self._cast_value(row[idx], col["tipo"])
                        index.add(value, row_number)

                    elif col["indice"] == "RTREE":
                        rtree_cols = col.get("rtree_cols") or []
                        if len(rtree_cols) != 2:
                            continue

                        idx_x = header_map.get(rtree_cols[0].lower())
                        idx_y = header_map.get(rtree_cols[1].lower())
                        if idx_x is None or idx_y is None or idx_x >= len(row) or idx_y >= len(row):
                            continue

                        index = self.indices[table_name].get(col["nombre"])
                        if index is None:
                            index = RTree(table_name)
                            self.indices[table_name][col["nombre"]] = index

                        point = (
                            self._cast_value(row[idx_x], col["tipo"]),
                            self._cast_value(row[idx_y], col["tipo"]),
                        )
                        index.add(point, row_number, 0)

        return row_number

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
        table_name = p[3].lower()
        columnas = p[5]
        csv_path = self._resolve_path(p[9])
        
        # 1. Guardar en el catálogo la estructura oficial
        self.catalog[table_name] = {
            "columnas": columnas,
            "csv_path": str(csv_path)
        }

        try:
            filas_indexadas = self._load_indices_from_csv(table_name, columnas, csv_path)
            p[0] = f"¡ÉXITO! Tabla '{table_name}' lista en disco. Se indexaron {filas_indexadas} filas."
                
        except Exception as e:
            p[0] = f"ERROR DE EJECUCIÓN O LECTURA CSV: {str(e)}"


    def p_sentencia_create_file_sin_archivos(self, p):
        """
        sentencia : CREATE TABLE ID LPAREN lista_columnas RPAREN
        """
        table_name = p[3].lower()
        columnas = p[5]

        # 1. Guardar en el catálogo
        self.catalog[table_name] = {"columnas": columnas}
        self.indices.setdefault(table_name, {})
        
        try:
            for col in columnas:
                if col['indice'] == 'BTREE':
                    self.indices[table_name][col['nombre']] = BPlusTreeIndex(
                        table_name,
                        self._index_name(table_name, col['nombre'], 'btree'),
                        col['tipo'],
                        60 if col['tipo'].upper() in {'VARCHAR', 'STR'} else 0,
                    )
                elif col['indice'] == 'HASH':
                    self.indices[table_name][col['nombre']] = ExtendibleHashing(
                        table_name,
                        self._index_name(table_name, col['nombre'], 'hash'),
                        col['tipo'],
                        key_size=60,
                    )
                elif col['indice'] == 'RTREE':
                    self.indices[table_name][col['nombre']] = RTree(table_name)
                    
            p[0] = f"¡ÉXITO! Tabla '{table_name}' inicializada físicamente en disco. (Vacía)"
        except Exception as e:
            p[0] = f"ERROR DE EJECUCIÓN: {str(e)}"

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

    def execute(self, query):
        return self.parser.parse(query)

    def execute_query(self, query):
        return self.execute(query)

# Pruebas rápidas
if __name__ == "__main__":
    db = SQLParser()
    ruta = "src/data/airbnb_database.csv"
    
    # ¡FÍJATE EN EL PUNTO Y COMA AL FINAL!
    sql = f"CREATE TABLE airbnb (id INT INDEX BTREE, nombre VARCHAR, ciudad VARCHAR, lat FLOAT, long FLOAT, precio FLOAT, tipo VARCHAR, cap INT) FROM FILE '{ruta}';"
    
    print("--- INICIANDO PARSER ---")
    resultados = db.execute(sql)
    
    print("\nRESULTADOS:")
    if resultados:
        for i, res in enumerate(resultados):
            print(f"Instrucción {i+1}: {res}")