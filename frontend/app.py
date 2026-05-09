import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

_HERE = Path(__file__).resolve().parent
_PROJECT_ROOT = _HERE.parent
_SRC = _PROJECT_ROOT / "src"
_INDICES = _SRC / "indices"
for _p in (str(_PROJECT_ROOT), str(_SRC), str(_INDICES)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.parser.sql_parser import SQLParser  # noqa: E402

st.set_page_config(
    page_title="Mini SGBD",
    page_icon=":floppy_disk:",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_parser() -> SQLParser:
    return SQLParser()


# Columnas reales del CSV `airbnb_database.csv`. Si el usuario crea su tabla con
# estas columnas (o un prefijo de ellas en el mismo orden), la ingesta funciona.
DATA_COLUMNS = ["id", "name", "city", "lat", "lng", "price", "room_type", "accommodates"]


CREATE_TEMPLATE = (
    "CREATE TABLE airbnb20k (\n"
    "  id INT INDEX BTREE,\n"
    "  name VARCHAR,\n"
    "  city VARCHAR INDEX BTREE,\n"
    "  lat FLOAT,\n"
    "  lng FLOAT,\n"
    "  price FLOAT,\n"
    "  room_type VARCHAR,\n"
    "  accommodates INT\n"
    ") FROM FILE 'airbnb_database.csv' WITH N=20000;"
)


def _filas_to_df(filas: list[list], columnas: list[str] | None = None) -> pd.DataFrame | None:
    if not filas:
        return None
    cols = columnas or DATA_COLUMNS
    if filas and len(filas[0]) != len(cols):
        cols = [f"c{i}" for i in range(len(filas[0]))]
    return pd.DataFrame(filas, columns=cols)


def execute_sql(sql: str) -> dict:
    parser = get_parser()
    t0 = time.time()
    try:
        results = parser.run(sql)
    except Exception as e:
        return {
            "data": None, "io": 0,
            "execution_time_ms": round((time.time() - t0) * 1000, 3),
            "indice_tipo": None,
            "message": f"Error parseando/ejecutando: {e}",
            "warning": None, "raw": None,
        }

    if not results:
        return {"data": None, "io": 0, "execution_time_ms": 0.0,
                "indice_tipo": None,
                "message": "Sin resultados.", "warning": None, "raw": None}
    res = results[-1]

    if not isinstance(res, dict):
        return {"data": None, "io": 0, "execution_time_ms": 0.0,
                "indice_tipo": None,
                "message": f"Resultado inesperado: {res!r}",
                "warning": None, "raw": None}

    if res.get("status") == "error":
        return {"data": None, "io": 0,
                "execution_time_ms": float(res.get("execution_time_ms", 0.0)),
                "indice_tipo": None,
                "message": f"Error: {res.get('msg', 'desconocido')}",
                "warning": None, "raw": res}

    op = res.get("op", "")
    disk = int(res.get("disk_accesses", 0) or 0)
    tms = float(res.get("execution_time_ms", 0.0) or 0.0)
    idx = res.get("indice_tipo")

    df = None
    warning = None
    if op == "SELECT_POINT":
        filas = res.get("filas", []) or []
        df = _filas_to_df(filas)
        msg = f"{len(filas)} fila(s) encontrada(s)."
    elif op == "SELECT_RANGE":
        filas = res.get("filas", []) or []
        df = _filas_to_df(filas)
        msg = f"{len(filas)} fila(s) en el rango."
        if idx == "HASH" and len(filas) == 0:
            warning = ("HASH no soporta búsqueda por rango. "
                       "Usá B+ Tree o Sequential para queries BETWEEN.")
    elif op == "INSERT":
        msg = f"1 fila insertada en RID {res.get('rid')}."
    elif op == "DELETE":
        msg = f"{res.get('borrados', 0)} fila(s) eliminada(s)."
    elif op == "CREATE_TABLE":
        n_rows = res.get("n_rows", 0)
        msg = (f"Tabla '{res.get('tabla')}' creada con {n_rows:,} filas. "
               f"Índices: {res.get('indices', {})}.")
    elif op == "SHOW_TABLES":
        df = _filas_to_df(res.get("filas", []), res.get("columnas"))
        msg = f"{res.get('n', 0)} tabla(s) en el catálogo."
    elif op == "VIEW_INDICES":
        df = _filas_to_df(res.get("filas", []), res.get("columnas"))
        msg = f"Esquema de '{res.get('tabla')}'."
    elif op == "DROP_TABLE":
        eliminados = res.get("archivos_eliminados") or []
        msg = (f"Tabla '{res.get('tabla')}' eliminada "
               f"({len(eliminados)} archivo(s)/carpeta(s) borrados).")
    else:
        msg = f"OP={op} status={res.get('status')}"

    return {
        "data": df,
        "io": disk,
        "execution_time_ms": tms,
        "indice_tipo": idx,
        "message": msg,
        "warning": warning,
        "raw": res,
    }


if "last_result" not in st.session_state:
    st.session_state.last_result = {
        "data": None, "io": 0, "execution_time_ms": 0.0,
        "indice_tipo": None,
        "message": "Aún no se ha ejecutado ninguna consulta.",
        "warning": None, "raw": None,
    }
if "tables_known" not in st.session_state:
    st.session_state.tables_known = []
if "active_table" not in st.session_state:
    st.session_state.active_table = None
if "sql_text" not in st.session_state:
    st.session_state.sql_text = CREATE_TEMPLATE


def run_and_store(sql: str):
    res = execute_sql(sql)
    st.session_state.last_result = res
    raw = res.get("raw") or {}
    op = raw.get("op")
    if op == "CREATE_TABLE" and raw.get("status") == "ok":
        nombre = raw.get("tabla")
        if nombre and nombre not in st.session_state.tables_known:
            st.session_state.tables_known.append(nombre)
        st.session_state.active_table = nombre
    if op == "SHOW_TABLES" and raw.get("status") == "ok":
        st.session_state.tables_known = [f[0] for f in raw.get("filas", [])]
    if op == "DROP_TABLE" and raw.get("status") == "ok":
        nombre = raw.get("tabla")
        if nombre in st.session_state.tables_known:
            st.session_state.tables_known.remove(nombre)
        if st.session_state.active_table == nombre:
            st.session_state.active_table = (
                st.session_state.tables_known[-1]
                if st.session_state.tables_known else None
            )


with st.sidebar:
    st.title(":floppy_disk: Mini SGBD")
    st.caption("UTEC - Base de Datos II")

    st.divider()
    st.subheader("Catálogo")
    st.caption(
        "Las tablas se crean exclusivamente desde la consola con "
        "`CREATE TABLE ... FROM FILE 'airbnb_database.csv' WITH N=<filas>;`. "
        "Cada tabla tiene su propio heap independiente."
    )
    if st.button("SHOW TABLES", width="stretch"):
        run_and_store("SHOW TABLES;")
        st.rerun()

    if st.session_state.tables_known:
        st.markdown("**Tablas detectadas:**")
        for t in st.session_state.tables_known:
            marca = " :star:" if t == st.session_state.active_table else ""
            st.markdown(f"- `{t}`{marca}")
        st.caption(":star: = última creada/usada en la consola")
    else:
        st.info("Aún no hay tablas. Ejecutá un CREATE TABLE en la consola.")


st.title("Mini SGBD - Consola")
st.caption("Frontend de consultas con métricas reales de I/O y latencia.")

res = st.session_state.last_result
m1, m2, m3 = st.columns(3)
m1.metric("Accesos totales a disco", res["io"])
m2.metric("Tiempo (ms)", f"{res['execution_time_ms']:.3f}")
m3.metric("Índice usado", res.get("indice_tipo") or "—")

st.info(res["message"])
if res.get("warning"):
    st.warning(res["warning"])
st.divider()


def _active_or(default: str) -> str:
    return st.session_state.active_table or default


def _build_templates() -> dict[str, str]:
    t = _active_or("airbnb20k")
    return {
        "CREATE 1k":     CREATE_TEMPLATE.replace("airbnb20k", "airbnb1k").replace("N=20000", "N=1000"),
        "CREATE 20k":    CREATE_TEMPLATE,
        "CREATE 100k":   CREATE_TEMPLATE.replace("airbnb20k", "airbnb100k").replace("N=20000", "N=100000"),
        "SHOW TABLES":   "SHOW TABLES;",
        "VIEW INDICES":  f"VIEW INDICES FROM {t};",
        "DROP TABLE":    f"DROP TABLE {t};",
        "SELECT id":     f"SELECT * FROM {t} WHERE id = 2577;",
        "SELECT city":   f"SELECT * FROM {t} WHERE city = 'Paris';",
        "RANGE id":      f"SELECT * FROM {t} WHERE id BETWEEN 1000 AND 5000;",
        "INSERT":        (f"INSERT INTO {t} VALUES "
                          "(99999, 'New', 'Lima', -12.1, -77.0, 85.0, 'Loft', 2);"),
        "DELETE":        f"DELETE FROM {t} WHERE id = 99999;",
    }


st.subheader("Consola SQL")
st.caption(
    "Tú escribes el SQL. Las plantillas se adaptan a la última tabla creada. "
    "Para cargar otra tabla repetí `CREATE TABLE ... WITH N=<filas>;` con un nombre distinto."
)

QUERY_TEMPLATES = _build_templates()
tpl_items = list(QUERY_TEMPLATES.items())
for row_idx in range(0, len(tpl_items), 5):
    bcols = st.columns(5)
    for j, (label, template) in enumerate(tpl_items[row_idx:row_idx + 5]):
        if bcols[j].button(label, width="stretch",
                           key=f"tpl_{label}"):
            st.session_state.sql_text = template
            st.rerun()

sql = st.text_area(
    "SQL",
    height=200,
    key="sql_text",
    placeholder=(
        "Ej.\n"
        "CREATE TABLE airbnb20k (id INT INDEX BTREE, name VARCHAR, ...) "
        "FROM FILE 'airbnb_database.csv' WITH N=20000;\n"
        "SHOW TABLES;\n"
        "VIEW INDICES FROM airbnb20k;\n"
        "SELECT * FROM airbnb20k WHERE id = 2577;"
    ),
)

if st.button("Ejecutar", type="primary", key="btn_run_sql"):
    if sql.strip():
        run_and_store(sql)
        st.rerun()
    else:
        st.warning("Escribe una consulta antes de ejecutar.")

if isinstance(res["data"], pd.DataFrame) and not res["data"].empty:
    st.dataframe(res["data"], width="stretch", hide_index=True)
