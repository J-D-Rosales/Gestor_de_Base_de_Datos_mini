import time

from executor import Executor


def make_create(tabla: str, csv: str):
    return {
        "tipo_operacion": "CREATE_TABLE",
        "tabla": tabla,
        "csv_path": csv,
        "columnas": [
            {"nombre": "id",        "tipo": "INT",     "indice": "SEQUENTIAL", "rtree_cols": None},
            {"nombre": "name",      "tipo": "VARCHAR", "indice": None,         "rtree_cols": None},
            {"nombre": "city",      "tipo": "VARCHAR", "indice": "BTREE",      "rtree_cols": None},
            {"nombre": "lat",       "tipo": "FLOAT",   "indice": None,         "rtree_cols": None},
            {"nombre": "long",      "tipo": "FLOAT",   "indice": None,         "rtree_cols": None},
            {"nombre": "price",     "tipo": "FLOAT",   "indice": "HASH",       "rtree_cols": None},
            {"nombre": "room_type", "tipo": "VARCHAR", "indice": None,         "rtree_cols": None},
            {"nombre": "cap",       "tipo": "INT",     "indice": None,         "rtree_cols": None},
        ],
    }


def fmt(res, key="filas", limit=2):
    if isinstance(res, dict):
        if res.get("status") != "ok":
            return res
        out = {k: v for k, v in res.items() if k not in (key,)}
        rows = res.get(key, [])
        out[key] = rows[:limit]
        out["n"] = len(rows)
        return out
    return res


def run_suite(ex: Executor, tabla: str, csv: str):
    print("\n" + "=" * 70)
    print(f"  DATASET {csv}  (tabla={tabla})")
    print("=" * 70)

    t0 = time.perf_counter()
    res = ex.execute(make_create(tabla, csv))
    dt = time.perf_counter() - t0
    print(f"\n[CREATE TABLE] {dt:.2f}s wall-clock")
    print({k: res.get(k) for k in ("status", "indices", "backend")})
    if res.get("status") != "ok":
        print("CREATE TABLE falló, salto consultas.")
        return

    # SEQUENTIAL: point por id
    print(f"\n[SEQUENTIAL] SELECT * FROM {tabla} WHERE id = 2577")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "id", "operador": "=", "valor": 2577},
    })))

    # SEQUENTIAL: range por id
    print(f"\n[SEQUENTIAL] SELECT * FROM {tabla} WHERE id BETWEEN 2577 AND 2600")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_RANGE", "tabla": tabla,
        "condicion": {"columna_where": "id", "valor_inicio": 2577, "valor_fin": 2600},
    })))

    # BTREE: point por city
    print(f"\n[BTREE] SELECT * FROM {tabla} WHERE city = 'Paris'")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "city", "operador": "=", "valor": "Paris"},
    })))

    # HASH: point por price
    print(f"\n[HASH] SELECT * FROM {tabla} WHERE price = 125.0")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "price", "operador": "=", "valor": 125.0},
    })))

    # INSERT
    print(f"\n[INSERT] nueva fila id=999999")
    print(ex.execute({
        "tipo_operacion": "INSERT", "tabla": tabla,
        "valores_a_insertar": [999999, "TestRow", "TestCity", 12.34, 56.78, 777.0,
                               "Entire place", 2],
    }))

    # Verificar via cada índice
    print(f"\n[SEQUENTIAL] verificar id=999999")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "id", "operador": "=", "valor": 999999},
    })))
    print(f"\n[BTREE] verificar city='TestCity'")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "city", "operador": "=", "valor": "TestCity"},
    })))
    print(f"\n[HASH] verificar price=777.0")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "price", "operador": "=", "valor": 777.0},
    })))

    # DELETE por SEQUENTIAL
    print(f"\n[DELETE] WHERE id = 999999  (vía SEQUENTIAL)")
    print(ex.execute({
        "tipo_operacion": "DELETE", "tabla": tabla,
        "condicion": {"columna_where": "id", "operador": "=", "valor": 999999},
    }))

    # Confirmar que ya no aparece
    print(f"\n[SEQUENTIAL] post-delete id=999999 (esperado n=0)")
    print(fmt(ex.execute({
        "tipo_operacion": "SELECT_POINT", "tabla": tabla,
        "condicion": {"columna_where": "id", "operador": "=", "valor": 999999},
    })))


CASOS = {
    "1k":   ("airbnb_1k",   "airbnb_1000.csv"),
    "10k":  ("airbnb_10k",  "airbnb_10000.csv"),
    "100k": ("airbnb_100k", "airbnb_100000.csv"),
}


def main(sizes=None):
    """Corre la suite. `sizes` es lista de claves '1k'/'10k'/'100k'.
    Sin argumento, corre los 3."""
    ex = Executor()
    if not sizes:
        sizes = list(CASOS.keys())
    for s in sizes:
        if s not in CASOS:
            print(f"tamaño desconocido: {s} (válidos: {list(CASOS)})")
            continue
        tabla, csv = CASOS[s]
        run_suite(ex, tabla, csv)


if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
