"""
Smoke test: CREATE TABLE FROM FILE para airbnb_1k / 10k / 100k con
B+Tree sobre id y sobre city, y un par de SELECTs por cada índice.
"""
import time

from executor import Executor


def make_create(tabla: str, csv: str) -> dict:
    """
    Equivalente AST a:
    CREATE TABLE <tabla> (
        id INT INDEX BTREE,
        name VARCHAR,
        city VARCHAR INDEX BTREE,
        lat FLOAT, long FLOAT, price FLOAT,
        room_type VARCHAR, cap INT
    ) FROM FILE '<csv>';
    """
    return {
        "tipo_operacion": "CREATE_TABLE",
        "tabla": tabla,
        "csv_path": csv,
        "columnas": [
            {"nombre": "id",        "tipo": "INT",     "indice": "BTREE", "rtree_cols": None},
            {"nombre": "name",      "tipo": "VARCHAR", "indice": None,    "rtree_cols": None},
            {"nombre": "city",      "tipo": "VARCHAR", "indice": "BTREE", "rtree_cols": None},
            {"nombre": "lat",       "tipo": "FLOAT",   "indice": None,    "rtree_cols": None},
            {"nombre": "long",      "tipo": "FLOAT",   "indice": None,    "rtree_cols": None},
            {"nombre": "price",     "tipo": "FLOAT",   "indice": None,    "rtree_cols": None},
            {"nombre": "room_type", "tipo": "VARCHAR", "indice": None,    "rtree_cols": None},
            {"nombre": "cap",       "tipo": "INT",     "indice": None,    "rtree_cols": None},
        ],
    }


def select_by_id(ex, tabla, valor):
    return ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "id", "operador": "=", "valor": valor},
    })


def select_by_city(ex, tabla, ciudad: str):
    # El BPlusTree para STR pad/trunca a key_size; passamos el string crudo.
    return ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "city", "operador": "=", "valor": ciudad},
    })


def main():
    ex = Executor()
    casos = [
        ("airbnb_1k",   "airbnb_1000.csv"),
        ("airbnb_10k",  "airbnb_10000.csv"),
        ("airbnb_100k", "airbnb_100000.csv"),
    ]

    for tabla, csv in casos:
        print(f"\n========== {tabla}  ({csv}) ==========")
        t0 = time.perf_counter()
        res = ex.execute(make_create(tabla, csv))
        dt = (time.perf_counter() - t0)
        print(f"CREATE TABLE -> {dt:.2f}s")
        for k in ("status", "columnas", "formato", "record_size", "indices", "backend"):
            print(f"  {k}: {res.get(k)}")

        # SELECT por id (B+Tree INT)
        r1 = select_by_id(ex, tabla, 2577)
        print(f"  SELECT id=2577        n={r1.get('n')}  primera={(r1.get('filas') or [None])[0]}")

        # SELECT por city (B+Tree STR)
        r2 = select_by_city(ex, tabla, "Paris")
        filas = r2.get("filas") or []
        print(f"  SELECT city='Paris'   n={r2.get('n')}  ejemplo={filas[0] if filas else None}")


if __name__ == "__main__":
    main()
