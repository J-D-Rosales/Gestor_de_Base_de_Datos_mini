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


TABLE_NAME = "airbnb"
DATA_COLUMNS = ["id", "name", "city", "lat", "lng", "price", "room_type", "accommodates"]
INDEX_OPTIONS = {
    "B+ Tree":         "BTREE",
    "Extendible Hash": "HASH",
    "Sequential":      "SEQUENTIAL",
}


INDEXABLE_COLUMNS = ["id", "name", "city", "room_type", "accommodates"]


def _create_table_sql(csv_filename: str, primary_index: str,
                      cols_to_index: list[str]) -> str:
    pieces = []
    for c in DATA_COLUMNS:
        if c == "id":
            tipo = "INT"
        elif c in ("lat", "lng", "price"):
            tipo = "FLOAT"
        elif c == "accommodates":
            tipo = "INT"
        else:
            tipo = "VARCHAR"
        if c in cols_to_index:
            pieces.append(f"{c} {tipo} INDEX {primary_index}")
        else:
            pieces.append(f"{c} {tipo}")
    cols_sql = ", ".join(pieces)
    return f"CREATE TABLE {TABLE_NAME} ({cols_sql}) FROM FILE '{csv_filename}';"


def _csv_filename(dataset_size: int) -> str:
    return {
        1_000:   "airbnb_1000.csv",
        10_000:  "airbnb_10000.csv",
        100_000: "airbnb_100000.csv",
    }[dataset_size]


def _filas_to_df(filas: list[list]) -> pd.DataFrame | None:
    if not filas:
        return None
    return pd.DataFrame(filas, columns=DATA_COLUMNS)

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
        msg = (f"Tabla '{res.get('tabla')}' creada. "
               f"Índices: {res.get('indices', {})}.")
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
if "table_loaded" not in st.session_state:
    st.session_state.table_loaded = False
if "active_index" not in st.session_state:
    st.session_state.active_index = None
if "active_size" not in st.session_state:
    st.session_state.active_size = None
if "sql_text" not in st.session_state:
    st.session_state.sql_text = ""


def run_and_store(sql: str):
    res = execute_sql(sql)
    st.session_state.last_result = res


with st.sidebar:
    st.title(":floppy_disk: Mini SGBD")
    st.caption("UTEC - Base de Datos II")

    st.divider()
    st.subheader("Dataset")
    dataset_size = st.radio(
        "Número de registros",
        options=[1_000, 10_000, 100_000],
        format_func=lambda x: f"{x:,}",
        index=0,
        horizontal=True,
        key="dataset_size",
    )

    st.subheader("Índice de prueba")
    index_label = st.selectbox(
        "Técnica",
        options=list(INDEX_OPTIONS.keys()),
        index=0,
        key="index_label",
        help=("Define qué tipo de índice se construye. Cambiá la opción y "
              "volvé a cargar el dataset para comparar entre técnicas."),
    )
    index_code = INDEX_OPTIONS[index_label]

    cols_to_index = st.multiselect(
        "Columnas a indexar",
        options=INDEXABLE_COLUMNS,
        default=["id", "city"],
        help=("El índice elegido se construye sobre cada columna seleccionada. "
              "Las queries WHERE <col> = X / BETWEEN sobre esas columnas "
              "usarán ese índice. Más columnas = carga más lenta."),
    )
    if not cols_to_index:
        st.error("Seleccioná al menos una columna a indexar.")

    if st.button("Cargar dataset", use_container_width=True, type="primary",
                 disabled=not cols_to_index):
        csv_name = _csv_filename(dataset_size)
        sql = _create_table_sql(csv_name, index_code, cols_to_index)
        with st.spinner(
            f"Cargando {csv_name} con índice {index_code} sobre "
            f"{', '.join(cols_to_index)}..."
        ):
            res = execute_sql(sql)
        st.session_state.last_result = res
        if res["raw"] and res["raw"].get("status") == "ok":
            st.session_state.table_loaded = True
            st.session_state.active_index = index_code
            st.session_state.active_size = dataset_size
            st.session_state.active_cols = list(cols_to_index)
            st.success(res["message"])
        else:
            st.session_state.table_loaded = False
            st.error(res["message"])

    st.divider()
    st.caption("Estado actual")
    if st.session_state.table_loaded:
        cols = ", ".join(st.session_state.get("active_cols", []))
        st.success(
            f"Tabla `{TABLE_NAME}` cargada\n\n"
            f"- Filas: **{st.session_state.active_size:,}**\n"
            f"- Índice **{st.session_state.active_index}** sobre: **{cols}**"
        )
    else:
        st.warning("Pulsa 'Cargar dataset' antes de consultar.")

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

QUERY_TEMPLATES = {
    "SELECT id":     f"SELECT * FROM {TABLE_NAME} WHERE id = 2577;",
    "SELECT city":   f"SELECT * FROM {TABLE_NAME} WHERE city = 'Paris';",
    "SELECT room":   f"SELECT * FROM {TABLE_NAME} WHERE room_type = 'Entire place';",
    "SELECT accom":  f"SELECT * FROM {TABLE_NAME} WHERE accommodates = 4;",
    "RANGE id":      f"SELECT * FROM {TABLE_NAME} WHERE id BETWEEN 1000 AND 5000;",
    "RANGE accom":   f"SELECT * FROM {TABLE_NAME} WHERE accommodates BETWEEN 2 AND 4;",
    "INSERT":        (f"INSERT INTO {TABLE_NAME} VALUES "
                      "(99999, 'New', 'Lima', -12.1, -77.0, 85.0, 'Loft', 2);"),
    "DELETE":        f"DELETE FROM {TABLE_NAME} WHERE id = 99999;",
}

st.subheader("Consola SQL")
st.caption(
    "Tú escribes el SQL. Los botones de abajo solo pegan plantillas en el "
    "editor — luego puedes editarlas antes de ejecutar. "
    "Las queries sobre `id` usarán el índice configurado en el sidebar."
)

tpl_items = list(QUERY_TEMPLATES.items())
for row_idx in range(0, len(tpl_items), 4):
    bcols = st.columns(4)
    for j, (label, template) in enumerate(tpl_items[row_idx:row_idx + 4]):
        if bcols[j].button(label, use_container_width=True,
                           key=f"tpl_{label}"):
            st.session_state.sql_text = template
            st.rerun()

sql = st.text_area(
    "SQL",
    height=140,
    key="sql_text",
    placeholder=(
        "Ej.\n"
        f"SELECT * FROM {TABLE_NAME} WHERE id = 2577;\n"
        f"SELECT * FROM {TABLE_NAME} WHERE id BETWEEN 1000 AND 5000;\n"
        f"INSERT INTO {TABLE_NAME} VALUES (...);\n"
        f"DELETE FROM {TABLE_NAME} WHERE id = ...;"
    ),
)

if st.button("Ejecutar", type="primary", key="btn_run_sql"):
    if sql.strip():
        run_and_store(sql)
        st.rerun()
    else:
        st.warning("Escribe una consulta antes de ejecutar.")

if isinstance(res["data"], pd.DataFrame) and not res["data"].empty:
    st.dataframe(res["data"], use_container_width=True, hide_index=True)
