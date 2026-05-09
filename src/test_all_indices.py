import sys
from pathlib import Path

# --- CONFIGURACIÓN DINÁMICA DE RUTAS ---
_current_dir = Path(__file__).resolve().parent
if str(_current_dir.parent) not in sys.path:
    sys.path.insert(0, str(_current_dir.parent))
if str(_current_dir) not in sys.path:
    sys.path.insert(0, str(_current_dir))

from src.parser.sql_parser import SQLParser

N_VALUES = [1000, 5000, 10000, 50000, 100000]

INDEX_CONFIG = {
    "B+Tree": "BTREE",
    "Secuencial": "SEQUENTIAL",
    "Hash Ext.": "HASH"
}

def create_table_sql(table_name, n, index_type):
    csv_path = str(_current_dir / "data" / "bench" / f"airbnb_{n}.csv")
    if index_type == "RTREE":
        return f"CREATE TABLE {table_name} (id INT, name VARCHAR, city VARCHAR, lat FLOAT, long FLOAT, price FLOAT, room_type VARCHAR, accommodates INT, ubicacion POINT INDEX RTREE) FROM FILE '{csv_path}';"
    else:
        return f"CREATE TABLE {table_name} (id INT INDEX {index_type}, name VARCHAR, city VARCHAR, lat FLOAT, long FLOAT, price FLOAT, room_type VARCHAR, accommodates INT) FROM FILE '{csv_path}';"

def execute_sql(parser, sql):
    res = parser.run(sql)
    if isinstance(res, list) and len(res) > 0:
        return res[0]
    elif isinstance(res, dict):
        return res
    return {}

def run_test():
    parser = SQLParser()
    
    times = {n: {"B+Tree": {}, "Secuencial": {}, "Hash Ext.": {}, "R-Tree": {}} for n in N_VALUES}
    reads = {n: {"B+Tree": {}, "Secuencial": {}, "Hash Ext.": {}, "R-Tree": {}} for n in N_VALUES}

    for n in N_VALUES:
        print(f"\n{'='*50}\n🚀 INICIANDO PRUEBAS PARA N = {n}\n{'='*50}")
        
        # 1. BTree, Secuencial, Hash
        for label, index_type in INDEX_CONFIG.items():
            table_name = f"airbnb_{index_type}_{n}".lower()
            
            # CREATE
            print(f"[*] Inicializando {label}...")
            execute_sql(parser, create_table_sql(table_name, n, index_type))
            
            # RANGE
            print(f"   -> Range Search...")
            if label == "Hash Ext.":
                times[n][label]["Range"] = "-"
                reads[n][label]["Range"] = "-"
            else:
                res_range = execute_sql(parser, f"SELECT * FROM {table_name} WHERE id BETWEEN 1000 AND 5000;")
                if res_range.get("status") == "ok" and "error" not in str(res_range.get("data", "")):
                    times[n][label]["Range"] = res_range.get("execution_time_ms", "Error")
                    reads[n][label]["Range"] = res_range.get("disk_accesses", "Error")
                else:
                    times[n][label]["Range"] = "Error"
                    reads[n][label]["Range"] = "Error"

            # INSERT
            print(f"   -> Insert...")
            res_insert = execute_sql(parser, f"INSERT INTO {table_name} VALUES (99999, 'New', 'Lima', -12.1, -77.0, 85.0, 'Loft', 2);")
            if res_insert.get("status") == "ok":
                times[n][label]["Insert"] = res_insert.get("execution_time_ms", "Error")
                reads[n][label]["Insert"] = res_insert.get("disk_accesses", "Error")
            else:
                times[n][label]["Insert"] = "Error"
                reads[n][label]["Insert"] = "Error"

        # 2. R-Tree
        rtree_table = f"airbnb_rtree_{n}"
        print(f"[*] Inicializando R-Tree...")
        res_create_rtree = execute_sql(parser, create_table_sql(rtree_table, n, "RTREE"))
        if res_create_rtree.get("status") == "error":
            print(f"   [!] Error fatal creando R-Tree: {res_create_rtree.get('msg')}")
        
        # INSERT R-TREE
        print(f"   -> Insert R-Tree...")
        res_insert_rtree = execute_sql(parser, f"INSERT INTO {rtree_table} VALUES (99999, 'New', 'Lima', -12.1, -77.0, 85.0, 'Loft', 2);")
        if res_insert_rtree.get("status") == "ok":
            times[n]["R-Tree"]["Insert"] = res_insert_rtree.get("execution_time_ms", "Error")
            reads[n]["R-Tree"]["Insert"] = res_insert_rtree.get("disk_accesses", "Error")
        else:
            times[n]["R-Tree"]["Insert"] = "Error"
            reads[n]["R-Tree"]["Insert"] = "Error"

        # KNN R-TREE
        print(f"   -> KNN Search...")
        res_knn = execute_sql(parser, f"SELECT * FROM {rtree_table} WHERE ubicacion IN (POINT(-12.1, -77.0), K 5);")
        if res_knn.get("status") == "ok":
            times[n]["R-Tree"]["KNN"] = res_knn.get("execution_time_ms", "Error")
            reads[n]["R-Tree"]["KNN"] = res_knn.get("disk_accesses", "Error")
        else:
            print(f"   [!] Error en KNN: {res_knn.get('msg', 'Fallo desconocido')}")
            times[n]["R-Tree"]["KNN"] = "Error"
            reads[n]["R-Tree"]["KNN"] = "Error"

    # --- IMPRIMIR TABLAS ---
    def print_table(title, columns, op_key):
        print(f"\n{'#'*80}\n{title}\n{'#'*80}")
        header = f"{'N Registros':<12} | " + " | ".join([f"{col:<15}" for col in columns])
        print(header)
        print("-" * len(header))
        for n in N_VALUES:
            row = f"{n:<12} | " + " | ".join([f"{str(times[n][col].get(op_key, '-')):<15}" for col in columns])
            print(row)

    print_table("TABLA 1: BÚSQUEDA POR RANGO (ms) [B+Tree y Secuencial]", ["B+Tree", "Secuencial"], "Range")
    print_table("TABLA 2: INSERCIÓN INDIVIDUAL (ms) [Todos los Índices]", ["B+Tree", "Secuencial", "Hash Ext.", "R-Tree"], "Insert")
    print_table("TABLA 3: BÚSQUEDA KNN (ms) [Solo R-Tree]", ["R-Tree"], "KNN")

    print("\n\n(Para Accesos a Disco I/O, el formato es idéntico pero leyendo el diccionario 'reads')")

if __name__ == "__main__":
    run_test()