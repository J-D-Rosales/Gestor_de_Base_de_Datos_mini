
# 🗄️ Mini SGBD - Simulador de Gestor de Base de Datos

Este proyecto es un motor de base de datos educativo desarrollado para el curso de **Base de Datos 2**. El objetivo es implementar estructuras de indexación clásicas (B+ Tree, Hash Extensible, Secuencial, R-Tree) garantizando un acceso a disco eficiente mediante **paginación de 4KB**, junto con un parser SQL y un frontend de consulta interactivo.

## 🏗️ Estructura del Proyecto

```text
proyecto_sgbd/
├── frontend/
│   ├── app.py                    # Consola Streamlit con métricas de I/O y latencia.
│   └── requirements.txt
├── src/
│   ├── buffer_manager.py         # I/O paginado de 4KB con caché LRU.
│   ├── executor.py               # Ejecuta planes (CREATE/SELECT/INSERT/DELETE).
│   ├── transaction_manager.py    # Soporte de transacciones.
│   ├── data/                     # Datasets de muestra y archivos generados.
│   │   ├── airbnb_1000.csv       # Dataset versionado (1k filas).
│   │   └── tables/               # (generado) heaps por tabla.
│   ├── indices/
│   │   ├── base_index.py         # Plantilla obligatoria para todos los índices.
│   │   ├── heap_file.py          # Almacenamiento primario de tuplas.
│   │   ├── sequential.py         # Archivo secuencial.
│   │   ├── sequential_index.py   # Índice secundario sobre el heap.
│   │   ├── extendible_hashing.py # Hash extensible.
│   │   ├── bplus_tree.py         # B+ Tree.
│   │   └── r_tree.py             # R-Tree (datos espaciales).
│   ├── parser/
│   │   └── sql_parser.py         # Lex/Yacc → delega al Executor.
│   └── test_all_indices.py       # Suite unificada de tests.
├── Dockerfile
├── docker-compose.yml
└── .gitignore
```

> Los datasets `airbnb_10000.csv` / `airbnb_100000.csv`, los `.idx` del R-Tree y la carpeta `src/data/tables/` están ignorados por Git: se regeneran al cargar el dataset desde el frontend.

---

## 🏛️ Arquitectura

El flujo de una consulta es:

```
SQLParser  →  Executor  →  Indices / HeapFile  →  BufferManager  →  disco
```

- **SQLParser** (`src/parser/sql_parser.py`): tokeniza/parsea SQL y produce un plan; ya **no** ejecuta nada por sí mismo.
- **Executor** (`src/executor.py`): mantiene catálogo, instancia los índices según `CREATE TABLE ... INDEX <tipo>`, y aplica `SELECT`/`INSERT`/`DELETE` sobre el heap y los índices.
- **Indices**: cada técnica recibe su `filepath` desde el Executor y devuelve `{data, disk_accesses, execution_time_ms}`.
- **BufferManager**: única vía de acceso a disco, con caché LRU y política write-back.

---

## 🚀 Guía de Inicio Rápido

### 1. Clonar y configurar el entorno
```bash
git clone <url-del-repositorio>
cd Gestor_de_Base_de_Datos_mini

python -m venv venv
# Linux/Mac:
source venv/bin/activate
# Windows:
.\venv\Scripts\activate

pip install -r frontend/requirements.txt
```

### 2. Ejecutar el frontend Streamlit
```bash
streamlit run frontend/app.py
```
Abre http://localhost:8501. En el sidebar elige tamaño de dataset, técnica de índice y columnas a indexar; luego escribe SQL en la consola.

### 3. Alternativa: Docker
```bash
docker compose up --build
```
Expone el frontend en http://localhost:8501.

---

## 🛠️ Instrucciones para Desarrolladores (Índices)

### 1. Regla de Oro: Acceso a Disco
**Está prohibido cargar archivos completos en memoria.** Toda lectura/escritura debe pasar por el `BufferManager`:
- `self.buffer.read_page(page_id)`
- `self.buffer.write_page(page_id, data_bytes)`

### 2. Formato de Salida
Todas las funciones (`add`, `search`, `range_search`, `remove`) deben devolver:
```python
{
    "data": <resultado_de_la_operacion>,
    "disk_accesses": <total_de_lecturas_y_escrituras>,
    "execution_time_ms": <tiempo_en_milisegundos>
}
```

### 3. Constructor
El índice debe aceptar `filepath` desde fuera (lo inyecta el Executor) en vez de cablearlo internamente.

### 4. Flujo de Trabajo en Git
1. Crea una rama: `git checkout -b feature/nombre-tecnica`.
2. Trabaja en tu archivo dentro de `src/indices/`.
3. `commit` + `push` y abre un Pull Request hacia `main`.

---

## 📝 Especificaciones Técnicas
- **Lenguaje:** Python 3.10+
- **Tamaño de Página:** 4096 bytes (4 KB).
- **Persistencia:** archivos binarios (`.heap`, `.bin`, `.idx`) bajo `src/data/`.
- **Frontend:** Streamlit.
- **Contenedor:** Docker / docker-compose.

---

## 👥 Integrantes y Responsabilidades
- **Persona A:** Archivo Secuencial e Interfaz Gráfica.
- **Persona B:** Hash Extensible.
- **Persona C:** B+ Tree.
- **Persona D:** R-Tree (Datos Espaciales).
- **Persona E (Arquitecto):** SQL Parser, Executor, Buffer Manager y Coordinación.

---

### 💡 Tips para el equipo
- No borren los `__init__.py` de las subcarpetas.
- Usen `struct` para serializar a bytes antes de pasar al `BufferManager`.
- Si necesitan un dataset mayor a 1k filas, generénlo localmente: el `.gitignore` ya excluye los grandes.
