import math
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
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
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_parser() -> SQLParser:
    return SQLParser()


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

CREATE_RTREE_TEMPLATE = (
    "CREATE TABLE airbnb_geo (\n"
    "  id INT INDEX BTREE,\n"
    "  name VARCHAR,\n"
    "  city VARCHAR,\n"
    "  lat FLOAT,\n"
    "  lng FLOAT,\n"
    "  price FLOAT,\n"
    "  room_type VARCHAR,\n"
    "  accommodates INT,\n"
    "  location POINT INDEX RTREE(lat, lng)\n"
    ") FROM FILE 'airbnb_database.csv' WITH N=5000;"
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
            "warning": None, "raw": None, "spatial": None,
        }

    if not results:
        return {"data": None, "io": 0, "execution_time_ms": 0.0,
                "indice_tipo": None,
                "message": "Sin resultados.", "warning": None, "raw": None, "spatial": None}
    res = results[-1]

    if not isinstance(res, dict):
        return {"data": None, "io": 0, "execution_time_ms": 0.0,
                "indice_tipo": None,
                "message": f"Resultado inesperado: {res!r}",
                "warning": None, "raw": None, "spatial": None}

    if res.get("status") == "error":
        return {"data": None, "io": 0,
                "execution_time_ms": float(res.get("execution_time_ms", 0.0)),
                "indice_tipo": None,
                "message": f"Error: {res.get('msg', 'desconocido')}",
                "warning": None, "raw": res, "spatial": None}

    op = res.get("op", "")
    disk = int(res.get("disk_accesses", 0) or 0)
    tms = float(res.get("execution_time_ms", 0.0) or 0.0)
    idx = res.get("indice_tipo")

    df = None
    warning = None
    spatial = None

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
    elif op in ("SELECT_SPATIAL_RADIUS", "SELECT_SPATIAL_KNN"):
        filas = res.get("filas", []) or []
        df = _filas_to_df(filas)
        n = len(filas)
        if op == "SELECT_SPATIAL_RADIUS":
            msg = f"{n} punto(s) dentro del radio {res.get('radio')}."
        else:
            msg = f"{n} vecino(s) más cercano(s) encontrado(s)."
        spatial = {
            "op": op,
            "punto": res.get("punto"),
            "puntos_resultado": res.get("puntos_resultado", []),
            "radio": res.get("radio"),
            "k": res.get("k"),
            "rtree_cols": res.get("rtree_cols", ["lat", "lng"]),
            "tabla": res.get("tabla", ""),
        }
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
        "spatial": spatial,
    }


# ── Funciones de visualización ──────────────────────────────────────────────

_LEVEL_PALETTE = [
    ("rgba(255,165,0,0.12)",  "rgba(255,140,0,0.85)",  3),   # nivel 0: naranja
    ("rgba(100,149,237,0.10)", "rgba(65,105,225,0.75)", 2),  # nivel 1: azul
    ("rgba(50,205,50,0.10)",  "rgba(34,139,34,0.65)",  1),   # nivel 2: verde
    ("rgba(221,160,221,0.08)", "rgba(148,0,211,0.55)", 1),   # nivel 3: morado
    ("rgba(255,160,122,0.07)", "rgba(220,20,60,0.45)", 1),   # nivel 4+: rojo
]


def _rect_path(x0, y0, x1, y1):
    return [x0, x1, x1, x0, x0, None], [y0, y0, y1, y1, y0, None]


def _make_geo_map(spatial: dict) -> go.Figure:
    """Mapa geográfico con Scattermap/MapLibre (OpenStreetMap, sin API key)."""
    puntos = spatial.get("puntos_resultado") or []
    punto = spatial.get("punto")
    radio = spatial.get("radio")
    rtree_cols = spatial.get("rtree_cols", ["lat", "lng"])
    op = spatial.get("op", "")

    # puntos_resultado[i] = [col0_val, col1_val] = [lat, lng] para airbnb
    lats = [p[0] for p in puntos]
    lngs = [p[1] for p in puntos]

    fig = go.Figure()

    if lats:
        fig.add_trace(go.Scattermap(
            lat=lats, lon=lngs,
            mode="markers",
            marker=dict(size=9, color="crimson", opacity=0.85),
            name=f"Resultados ({len(lats)})",
            hovertemplate=f"{rtree_cols[0]}: %{{lat:.5f}}<br>{rtree_cols[1]}: %{{lon:.5f}}<extra></extra>",
        ))

    if punto:
        fig.add_trace(go.Scattermap(
            lat=[punto[0]], lon=[punto[1]],
            mode="markers",
            marker=dict(size=16, color="dodgerblue", symbol="star"),
            name="Punto consulta",
            hovertemplate=f"Consulta<br>{rtree_cols[0]}: {punto[0]:.5f}<br>{rtree_cols[1]}: {punto[1]:.5f}<extra></extra>",
        ))

    center_lat = punto[0] if punto else (sum(lats) / len(lats) if lats else 0)
    center_lon = punto[1] if punto else (sum(lngs) / len(lngs) if lngs else 0)

    zoom = 10
    if radio and radio > 2:
        zoom = 7
    elif radio and radio > 0.5:
        zoom = 9

    title = (f"Búsqueda por radio={radio}" if op == "SELECT_SPATIAL_RADIUS"
             else f"KNN k={spatial.get('k')}")

    fig.update_layout(
        map=dict(style="open-street-map",
                 center=dict(lat=center_lat, lon=center_lon),
                 zoom=zoom),
        margin=dict(l=0, r=0, t=36, b=0),
        height=480,
        title=dict(text=title, font_size=14),
        legend=dict(x=0, y=1, bgcolor="rgba(255,255,255,0.7)"),
    )
    return fig


def _make_rtree_structure(nodes: list, spatial: dict | None = None) -> go.Figure:
    """
    Visualización 2D de los MBRs del R-tree por nivel.
    Ejes: X = rtree_cols[1] (lng), Y = rtree_cols[0] (lat) — convención geográfica.
    El MBR guardado es (min_col0, min_col1, max_col0, max_col1).
    """
    rtree_cols = (spatial or {}).get("rtree_cols", ["lat", "lng"])
    puntos = (spatial or {}).get("puntos_resultado") or []
    punto = (spatial or {}).get("punto")
    radio = (spatial or {}).get("radio")

    by_level: dict[int, list] = defaultdict(list)
    for n in nodes:
        by_level[n["level"]].append(n)

    fig = go.Figure()

    # MBRs por nivel (un trace por nivel con separadores NaN para eficiencia)
    for level in sorted(by_level.keys()):
        fill_color, line_color, lw = _LEVEL_PALETTE[min(level, len(_LEVEL_PALETTE) - 1)]
        xs_int, ys_int = [], []
        xs_leaf, ys_leaf = [], []
        for node in by_level[level]:
            mbr = node["mbr"]  # (min_col0, min_col1, max_col0, max_col1)
            # Convención geográfica: x=col1 (lng), y=col0 (lat)
            px, py = _rect_path(mbr[1], mbr[0], mbr[3], mbr[2])
            if node["is_leaf"]:
                xs_leaf += px
                ys_leaf += py
            else:
                xs_int += px
                ys_int += py

        if xs_int:
            fig.add_trace(go.Scatter(
                x=xs_int, y=ys_int, mode="lines",
                line=dict(color=line_color, width=lw),
                fill="toself", fillcolor=fill_color,
                name=f"Nivel {level} (interno)",
                legendgroup=f"lv{level}",
                hoverinfo="skip",
            ))
        if xs_leaf:
            fig.add_trace(go.Scatter(
                x=xs_leaf, y=ys_leaf, mode="lines",
                line=dict(color=line_color, width=1, dash="dot"),
                fill="toself", fillcolor="rgba(0,0,0,0)",
                name=f"Nivel {level} (hoja)",
                legendgroup=f"lv{level}",
                hoverinfo="skip",
            ))

    # Puntos resultado
    if puntos:
        fig.add_trace(go.Scatter(
            x=[p[1] for p in puntos],
            y=[p[0] for p in puntos],
            mode="markers",
            marker=dict(size=7, color="crimson", symbol="circle",
                        line=dict(color="white", width=0.5)),
            name=f"Resultados ({len(puntos)})",
        ))

    # Punto consulta
    if punto:
        fig.add_trace(go.Scatter(
            x=[punto[1]], y=[punto[0]],
            mode="markers",
            marker=dict(size=14, color="dodgerblue", symbol="star",
                        line=dict(color="navy", width=1)),
            name="Punto consulta",
        ))

    # Círculo de radio (aproximación en espacio plano lat/lng)
    if radio is not None and punto:
        theta = [i * 2 * math.pi / 120 for i in range(121)]
        fig.add_trace(go.Scatter(
            x=[punto[1] + radio * math.cos(t) for t in theta],
            y=[punto[0] + radio * math.sin(t) for t in theta],
            mode="lines",
            line=dict(color="dodgerblue", width=2, dash="dash"),
            name=f"Radio = {radio}",
        ))

    n_nodes = len(nodes)
    n_leaves = sum(1 for n in nodes if n["is_leaf"])
    max_lv = max(by_level.keys()) if by_level else 0

    fig.update_layout(
        title=dict(
            text=f"R-Tree — {n_nodes} nodos ({n_leaves} hojas) · {max_lv + 1} nivel(es)",
            font_size=14,
        ),
        xaxis_title=rtree_cols[1] if len(rtree_cols) > 1 else "X",
        yaxis_title=rtree_cols[0] if rtree_cols else "Y",
        height=580,
        hovermode="closest",
        legend=dict(x=1.01, y=1, xanchor="left"),
        margin=dict(l=60, r=160, t=50, b=50),
    )
    return fig


# ── Session state ────────────────────────────────────────────────────────────

_DEFAULTS = {
    "last_result": {
        "data": None, "io": 0, "execution_time_ms": 0.0,
        "indice_tipo": None,
        "message": "Aún no se ha ejecutado ninguna consulta.",
        "warning": None, "raw": None, "spatial": None,
    },
    "tables_known": [],
    "active_table": None,
    "sql_text": CREATE_RTREE_TEMPLATE,
    "spatial_data": None,
    "rtree_viz_table": "",
    "rtree_viz_col": "",
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


def run_and_store(sql: str):
    res = execute_sql(sql)
    st.session_state.last_result = res
    if res.get("spatial"):
        st.session_state.spatial_data = res["spatial"]
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


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Mini SGBD")
    st.caption("UTEC - Base de Datos II")
    st.divider()
    st.subheader("Catálogo")
    st.caption(
        "Crea tablas con `CREATE TABLE ... FROM FILE 'airbnb_database.csv' WITH N=<filas>;`. "
        "Para R-Tree usá `location POINT INDEX RTREE(lat, lng)`."
    )
    if st.button("SHOW TABLES", width="stretch"):
        run_and_store("SHOW TABLES;")
        st.rerun()

    if st.session_state.tables_known:
        st.markdown("**Tablas detectadas:**")
        for t in st.session_state.tables_known:
            marca = " :star:" if t == st.session_state.active_table else ""
            st.markdown(f"- `{t}`{marca}")
        st.caption(":star: = última creada/usada")
    else:
        st.info("Aún no hay tablas. Ejecutá un CREATE TABLE.")

    st.divider()


# ── Área principal ────────────────────────────────────────────────────────────

st.title("Mini SGBD — Consola")
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
    t = _active_or("airbnb_geo")
    tg = "airbnb_geo"
    return {
        "CREATE (B+/Hash)":  CREATE_TEMPLATE,
        "CREATE (R-Tree)":   CREATE_RTREE_TEMPLATE,
        "SHOW TABLES":       "SHOW TABLES;",
        "VIEW INDICES":      f"VIEW INDICES FROM {t};",
        "DROP TABLE":        f"DROP TABLE {t};",
        "SELECT id":         f"SELECT * FROM {t} WHERE id = 2577;",
        "RANGE id":          f"SELECT * FROM {t} WHERE id BETWEEN 1000 AND 5000;",
        "RADIO (París)":     f"SELECT * FROM {tg} WHERE location IN (POINT(48.86, 2.35), RADIUS 0.1);",
        "KNN (París)":       f"SELECT * FROM {tg} WHERE location IN (POINT(48.86, 2.35), K 5);",
        "INSERT":            (f"INSERT INTO {t} VALUES "
                              "(99999, 'New', 'Lima', -12.1, -77.0, 85.0, 'Loft', 2);"),
        "DELETE":            f"DELETE FROM {t} WHERE id = 99999;",
    }


st.subheader("Consola SQL")
st.caption(
    "Las plantillas se adaptan a la última tabla creada. "
    "**RADIO** y **KNN** requieren una tabla con `POINT INDEX RTREE(lat, lng)`."
)

QUERY_TEMPLATES = _build_templates()
tpl_items = list(QUERY_TEMPLATES.items())
for row_idx in range(0, len(tpl_items), 4):
    bcols = st.columns(4)
    for j, (label, template) in enumerate(tpl_items[row_idx:row_idx + 4]):
        if bcols[j % 4].button(label, width="stretch", key=f"tpl_{label}"):
            st.session_state.sql_text = template
            st.rerun()

sql = st.text_area(
    "SQL",
    height=200,
    key="sql_text",
    placeholder=(
        "Ej.\n"
        "CREATE TABLE airbnb_geo (..., location POINT INDEX RTREE(lat, lng)) "
        "FROM FILE 'airbnb_database.csv' WITH N=5000;\n"
        "SELECT * FROM airbnb_geo WHERE location IN (POINT(48.86, 2.35), RADIUS 0.1);\n"
        "SELECT * FROM airbnb_geo WHERE location IN (POINT(48.86, 2.35), K 5);"
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


# ── Visualización espacial (resultado de la última consulta espacial) ─────────

spatial = st.session_state.get("spatial_data")
if spatial and spatial.get("puntos_resultado") is not None:
    st.divider()
    st.subheader("Visualización espacial")

    tab_map, tab_tree = st.tabs(["Mapa geográfico", "Estructura R-Tree"])

    with tab_map:
        st.caption(
            "Mapa OpenStreetMap con los puntos recuperados por la consulta espacial. "
            "Estrella azul = punto de consulta · Círculos rojos = resultados."
        )
        fig_map = _make_geo_map(spatial)
        st.plotly_chart(fig_map, width="stretch")

    with tab_tree:
        st.caption(
            "MBRs (Minimum Bounding Rectangles) del índice R-Tree. "
            "Cada nivel se muestra en un color distinto. "
            "Para cargar la estructura selecciona la tabla y columna en el sidebar."
        )
        viz_table = st.session_state.rtree_viz_table or spatial.get("tabla", "")
        viz_col = st.session_state.rtree_viz_col or "location"
        if viz_table and viz_col:
            try:
                rtree_data = get_parser().get_rtree_nodes(viz_table, viz_col)
                if rtree_data and rtree_data.get("nodes"):
                    nodes = rtree_data["nodes"]
                    # Limitar a 500 nodos para rendimiento
                    if len(nodes) > 500:
                        st.info(f"El R-Tree tiene {len(nodes)} nodos. Mostrando solo los primeros 3 niveles.")
                        max_lv = sorted({n["level"] for n in nodes})[2] if len({n["level"] for n in nodes}) > 2 else max(n["level"] for n in nodes)
                        nodes = [n for n in nodes if n["level"] <= max_lv]
                    fig_tree = _make_rtree_structure(nodes, spatial)
                    st.plotly_chart(fig_tree, width="stretch")
                    by_lv = defaultdict(int)
                    for n in rtree_data["nodes"]:
                        by_lv[n["level"]] += 1
                    st.caption("Nodos por nivel: " + " · ".join(f"Nivel {lv}: {cnt}" for lv, cnt in sorted(by_lv.items())))
                else:
                    st.info(f"No se encontró índice RTREE en tabla `{viz_table}`, columna `{viz_col}`. "
                            "Verifica el nombre en el sidebar.")
            except Exception as e:
                st.warning(f"No se pudo cargar la estructura del R-Tree: {e}")
        else:
            st.info("Indica la tabla y columna del índice R-Tree en el sidebar para ver los MBRs.")


# ── Visualización standalone del R-Tree (sin consulta previa) ─────────────────

elif st.session_state.rtree_viz_table and st.session_state.rtree_viz_col:
    st.divider()
    st.subheader("Estructura R-Tree")
    viz_table = st.session_state.rtree_viz_table
    viz_col = st.session_state.rtree_viz_col
    try:
        rtree_data = get_parser().get_rtree_nodes(viz_table, viz_col)
        if rtree_data and rtree_data.get("nodes"):
            nodes = rtree_data["nodes"]
            if len(nodes) > 500:
                st.info(f"El R-Tree tiene {len(nodes)} nodos. Mostrando solo los primeros 3 niveles.")
                max_lv_set = sorted({n["level"] for n in nodes})
                max_lv = max_lv_set[2] if len(max_lv_set) > 2 else max_lv_set[-1]
                nodes = [n for n in nodes if n["level"] <= max_lv]
            fig_tree = _make_rtree_structure(nodes)
            st.plotly_chart(fig_tree, width="stretch")
            by_lv = defaultdict(int)
            for n in rtree_data["nodes"]:
                by_lv[n["level"]] += 1
            st.caption("Nodos por nivel: " + " · ".join(f"Nivel {lv}: {cnt}" for lv, cnt in sorted(by_lv.items())))
        else:
            st.warning(f"No se encontró índice RTREE en `{viz_table}.{viz_col}`.")
    except Exception as e:
        st.warning(f"Error al cargar R-Tree: {e}")
