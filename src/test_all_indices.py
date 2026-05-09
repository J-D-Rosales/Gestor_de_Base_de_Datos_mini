from pathlib import Path
import csv
import sys

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[1]
    for p in (str(project_root), str(project_root / "src"), str(project_root / "src" / "indices")):
        if p not in sys.path:
            sys.path.insert(0, p)

from src.parser.sql_parser import SQLParser


RUNS = 5
N_VALUES = [1000, 5000, 10000, 50000, 100000]
BASE_DATASET = "src/data/airbnb_database.csv"
BENCH_DATA_DIR = "src/data/bench"

RANGE_SQL = "SELECT * FROM airbnb WHERE id BETWEEN 1000 AND 5000;"
INSERT_SQL_TEMPLATE = "INSERT INTO airbnb VALUES ({idv}, 'New', 'Lima', -12.1, -77.0, 85.0, 'Loft', 2);"
RTREE_SQL = "SELECT * FROM airbnb WHERE ubicacion IN (POINT(-12.1, -77.0), K 5);"

INDEX_CONFIG = {
    "B+tree": "BTREE",
    "Extendible Hashing": "HASH",
    "Sequential file": "SEQUENTIAL",
}


def run_sql(parser: SQLParser, sql: str) -> dict:
    out = parser.run(sql)
    if not out:
        return {"status": "error", "msg": "Sin salida del parser"}
    return out[0]


def avg(values):
    return sum(values) / len(values) if values else 0.0


def ensure_dataset_for_n(n: int) -> str:
    data_dir = Path(BENCH_DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / f"airbnb_{n}.csv"
    if out_path.exists():
        return str(out_path)

    src_path = Path(BASE_DATASET)
    if not src_path.exists():
        raise FileNotFoundError(f"No existe dataset base: {BASE_DATASET}")

    with src_path.open("r", encoding="utf-8", newline="") as fin, out_path.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        header = next(reader, None)
        if header is None:
            raise RuntimeError("CSV base vacío")
        writer.writerow(header)

        count = 0
        for row in reader:
            if not row:
                continue
            writer.writerow(row)
            count += 1
            if count >= n:
                break

    if count < n:
        raise RuntimeError(f"No hay suficientes filas para n={n}. Solo {count} disponibles.")

    return str(out_path)


def build_create_sql(index_kind: str, csv_path: str) -> str:
    return (
        "CREATE TABLE airbnb ("
        f"id INT INDEX {index_kind}, "
        "name VARCHAR, "
        "city VARCHAR, "
        "lat FLOAT, "
        "long FLOAT, "
        "price FLOAT, "
        "room_type VARCHAR, "
        "cap INT"
        f") FROM FILE '{csv_path}';"
    )


def build_create_sql_rtree(csv_path: str) -> str:
    return (
        "CREATE TABLE airbnb ("
        "id INT, "
        "name VARCHAR, "
        "city VARCHAR, "
        "lat FLOAT, "
        "long FLOAT, "
        "price FLOAT, "
        "room_type VARCHAR, "
        "cap INT, "
        "ubicacion FLOAT INDEX RTREE (lat, long)"
        f") FROM FILE '{csv_path}';"
    )


def benchmark_one_index_for_n(index_label: str, index_kind: str, csv_path: str, n: int):
    parser = SQLParser()
    created = run_sql(parser, build_create_sql(index_kind, csv_path))
    if created.get("status") != "ok":
        raise RuntimeError(f"CREATE falló ({index_label}, n={n}): {created}")

    range_times = []
    range_disks = []
    insert_times = []
    insert_disks = []

    for i in range(RUNS):
        res_range = run_sql(parser, RANGE_SQL)
        if res_range.get("status") != "ok":
            raise RuntimeError(f"SELECT_RANGE falló ({index_label}, n={n}, corrida {i+1}): {res_range}")
        range_times.append(float(res_range.get("execution_time_ms", 0.0) or 0.0))
        range_disks.append(float(res_range.get("disk_accesses", 0) or 0.0))

        insert_sql = INSERT_SQL_TEMPLATE.format(idv=99999 + i)
        res_insert = run_sql(parser, insert_sql)
        if res_insert.get("status") != "ok":
            raise RuntimeError(f"INSERT falló ({index_label}, n={n}, corrida {i+1}): {res_insert}")
        insert_times.append(float(res_insert.get("execution_time_ms", 0.0) or 0.0))
        insert_disks.append(float(res_insert.get("disk_accesses", 0) or 0.0))

    return {
        "SELECT_RANGE": {"time_ms": avg(range_times), "disk": avg(range_disks)},
        "INSERT": {"time_ms": avg(insert_times), "disk": avg(insert_disks)},
    }


def benchmark_rtree_for_n(csv_path: str, n: int):
    parser = SQLParser()
    created = run_sql(parser, build_create_sql_rtree(csv_path))
    if created.get("status") != "ok":
        raise RuntimeError(f"CREATE RTREE falló (n={n}): {created}")

    times = []
    disks = []
    n_rows = []

    for i in range(RUNS):
        res = run_sql(parser, RTREE_SQL)
        if res.get("status") != "ok":
            raise RuntimeError(f"RTREE kNN falló (n={n}, corrida {i+1}): {res}")
        times.append(float(res.get("execution_time_ms", 0.0) or 0.0))
        disks.append(float(res.get("disk_accesses", 0) or 0.0))
        n_rows.append(float(res.get("n", 0) or 0))

    return {
        "time_ms": avg(times),
        "disk": avg(disks),
        "rows": avg(n_rows),
    }


def print_query_table(title: str, query_key: str, metric_key: str, results_by_n: dict):
    print("\n" + title)
    print("n\tB+tree\tExtendible Hashing\tSequential file")
    for n in N_VALUES:
        row = results_by_n[n]
        print(
            f"{n}\t"
            f"{row['B+tree'][query_key][metric_key]:.4f}\t"
            f"{row['Extendible Hashing'][query_key][metric_key]:.4f}\t"
            f"{row['Sequential file'][query_key][metric_key]:.4f}"
        )


def print_rtree_table(rtree_results: dict):
    print("\nTABLA RTREE (kNN): SELECT * FROM airbnb WHERE ubicacion IN (POINT(-12.1, -77.0), K 5);")
    print("n\tTiempo promedio (ms)\tAccesos a disco promedio\tFilas promedio devueltas")
    for n in N_VALUES:
        row = rtree_results[n]
        print(f"{n}\t{row['time_ms']:.4f}\t{row['disk']:.4f}\t{row['rows']:.2f}")


def main():
    print("=" * 100)
    print("EXPERIMENTO MULTI-N (n = 1000, 5000, 10000, 50000, 100000)")
    print("Consultas SQL:")
    print(f"1) {RANGE_SQL}")
    print(f"2) {INSERT_SQL_TEMPLATE.format(idv=99999)}")
    print(f"3) {RTREE_SQL}  [solo RTREE]")
    print("=" * 100)

    datasets = {}
    for n in N_VALUES:
        datasets[n] = ensure_dataset_for_n(n)

    results_by_n = {}
    for n in N_VALUES:
        print(f"\n===== n = {n} =====")
        results_by_n[n] = {}
        for label, kind in INDEX_CONFIG.items():
            print(f"Probando {label}...")
            results_by_n[n][label] = benchmark_one_index_for_n(label, kind, datasets[n], n)

    rtree_results = {}
    for n in N_VALUES:
        print(f"Probando RTREE (n={n})...")
        rtree_results[n] = benchmark_rtree_for_n(datasets[n], n)

    print_query_table(
        "TABLA 1: SELECT * FROM airbnb WHERE id BETWEEN 1000 AND 5000;  (tiempo promedio ms)",
        "SELECT_RANGE",
        "time_ms",
        results_by_n,
    )
    print_query_table(
        "TABLA 2: INSERT INTO airbnb VALUES (...);  (tiempo promedio ms)",
        "INSERT",
        "time_ms",
        results_by_n,
    )

    print_query_table(
        "TABLA EXTRA: SELECT RANGE (accesos a disco promedio)",
        "SELECT_RANGE",
        "disk",
        results_by_n,
    )
    print_query_table(
        "TABLA EXTRA: INSERT (accesos a disco promedio)",
        "INSERT",
        "disk",
        results_by_n,
    )

    print_rtree_table(rtree_results)


if __name__ == "__main__":
    main()
