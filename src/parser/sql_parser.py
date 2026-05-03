import ply.yacc as yacc
import csv

from sql_lexer import tokens

class SQLParser:
    def __init__(self):
        self.catalog = {}
        self.tokens = tokens # Necesario para que yacc sepa qué tokens usar
        self.parser = yacc.yacc(module=self)

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
        csv_path = p[9]
        
        # 1. Guardar en el catálogo la estructura oficial
        self.catalog[table_name] = {
            "columnas": columnas,
            "csv_path": csv_path
        }
        
        cont = 0
        try:
            # Se realiza el codigo normal, en caso falle se da un exception y se hace la simulación
            try:
                from sequential_file import SequentialFile
                from bplus_tree import BPlusTree
                from extendible_hashing import ExtendibleHashing
                from r_tree import RTree
                
                # Inicializamos el archivo principal con todo el esquema y K (ej. K=100)
                archivo_principal = SequentialFile(table_name, columnas, k_limit=100)
                
                # Inicializamos los índices
                indices_activos = {}
                for col in columnas:
                    if col['indice'] == 'BTREE':
                        indices_activos[col['nombre']] = BPlusTree(table_name, col['nombre'], col['tipo'])
                    elif col['indice'] == 'HASH':
                        indices_activos[col['nombre']] = ExtendibleHashing(table_name, col['nombre'], col['tipo'])
                    elif col['indice'] == 'RTREE':
                        # Le mandamos el nombre de la tabla y los nombres de los ejes X e Y
                        indices_activos[col['nombre']] = RTree(table_name, col['rtree_cols'][0], col['rtree_cols'][1])
                
                # Carga masiva conectada a los archivos reales
                with open(csv_path, mode='r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    cabecera = next(reader, None)
                    
                    for fila in reader:
                        cont += 1
                        # guardar en archivo principal
                        puntero_fisico = archivo_principal.add(fila)
                        
                        # andar llaves a cada índice
                        for col in columnas:
                            if col['indice'] == 'BTREE' or col['indice'] == 'HASH':
                                indice_csv = cabecera.index(col['nombre'])
                                valor_llave = fila[indice_csv]
                                indices_activos[col['nombre']].add(key=valor_llave, pointer=puntero_fisico)
                                
                            elif col['indice'] == 'RTREE':
                                # El R-Tree necesita 2 datos (X, Y) para indexar, así que buscamos ambos en la fila usando la cabecera
                                idx_x = cabecera.index(col['rtree_cols'][0])
                                idx_y = cabecera.index(col['rtree_cols'][1])
                                valor_x = fila[idx_x]
                                valor_y = fila[idx_y]
                                # Pasamos una tupla (X, Y) como llave
                                indices_activos[col['nombre']].add(key=(valor_x, valor_y), pointer=puntero_fisico)
                                
                p[0] = f"¡ÉXITO! Tabla '{table_name}' lista en disco. Se indexaron {cont} filas."

            # === SI LOS ARCHIVOS FÍSICOS NO EXISTEN (SIMULACIÓN) ===
            except ImportError:
                with open(csv_path, mode='r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    cabecera = next(reader, None)
                    for fila in reader:
                        cont += 1
                        if cont <= 10:
                            print(f"   [SIMULACIÓN - DATO LEÍDO] ID: {fila[0]} -> {fila[1][:20]}...")
                            
                p[0] = f"¡ÉXITO! Tabla '{table_name}' lista. (Simulación: {cont} filas procesadas)"
                
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
        
        # === INTENTO DE USAR EL CÓDIGO REAL ===
        try:
            from sequential_file import SequentialFile
            from bplus_tree import BPlusTree
            from extendible_hashing import ExtendibleHashing
            from r_tree import RTree
            
            SequentialFile(table_name, columnas, k_limit=100)
            
            for col in columnas:
                if col['indice'] == 'BTREE':
                    BPlusTree(table_name, col['nombre'], col['tipo'])
                elif col['indice'] == 'HASH':
                    ExtendibleHashing(table_name, col['nombre'], col['tipo'])
                elif col['indice'] == 'RTREE':
                    RTree(table_name, col['rtree_cols'][0], col['rtree_cols'][1])
                    
            p[0] = f"¡ÉXITO! Tabla '{table_name}' inicializada físicamente en disco. (Vacía)"
            
        # === SIMULACIÓN ===
        except ImportError:
            p[0] = f"¡ÉXITO! Tabla '{table_name}' lista. (Simulación: Archivos no creados)"

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

# Pruebas rápidas
if __name__ == "__main__":
    db = SQLParser()
    ruta = "/home/daros/academico/2026-01/DB2/proyecto/src/data/airbnb_database.csv"
    
    # ¡FÍJATE EN EL PUNTO Y COMA AL FINAL!
    sql = f"CREATE TABLE airbnb (id INT INDEX BTREE, nombre VARCHAR, ciudad VARCHAR, lat FLOAT, long FLOAT, precio FLOAT, tipo VARCHAR, cap INT) FROM FILE '{ruta}';"
    
    print("--- INICIANDO PARSER ---")
    resultados = db.execute(sql)
    
    print("\nRESULTADOS:")
    if resultados:
        for i, res in enumerate(resultados):
            print(f"Instrucción {i+1}: {res}")