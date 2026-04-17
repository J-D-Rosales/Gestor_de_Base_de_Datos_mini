
# 🗄️ Mini SGBD - Simulador de Gestor de Base de Datos

Este proyecto es un motor de base de datos educativo desarrollado para el curso de **Base de Datos 2**. El objetivo es implementar estructuras de indexación clásicas (B+ Tree, Hash, Secuencial, R-Tree) garantizando un acceso a disco eficiente mediante **paginación de 4KB**.

## 🏗️ Estructura del Proyecto

El proyecto está organizado de forma modular para que cada integrante pueda trabajar en su estructura de datos sin interferir con los demás:

```text
proyecto_sgbd/
├── data/               # Archivos .dat generados (ignorar en Git)
├── src/
│   ├── buffer_manager.py  # ENTRADA/SALIDA: Maneja la lectura/escritura de páginas de 4KB.
│   ├── indices/           # ALGORITMOS: Aquí cada uno implementa su técnica.
│   │   ├── base_index.py  # Plantilla obligatoria para todos los índices.
│   │   ├── sequential.py  # Persona A
│   │   ├── extendible.py  # Persona B
│   │   ├── bplus_tree.py  # Persona C
│   │   └── r_tree.py      # Persona D
│   └── parser/            # SQL: Traduce comandos SQL a llamadas de los índices.
│       └── sql_parser.py
├── main.py             # Punto de entrada del programa.
└── .gitignore          # Archivos excluidos de Git (venv, __pycache__, .dat).
```

---

## 🚀 Guía de Inicio Rápido (Para el Equipo)

Sigue estos pasos para configurar tu entorno local:

### 1. Clonar y configurar el entorno
```bash
# Clonar el repositorio
git clone <url-del-repositorio>
cd proyecto_sgbd

# Crear entorno virtual (Solo una vez)
python3 -m venv venv

# Activar el entorno virtual
# En Linux/Mac:
source venv/bin/activate
# En Windows:
.\venv\Scripts\activate
```

### 2. Ejecutar el sistema
```bash
python main.py
```

---

## 🛠️ Instrucciones para Desarrolladores (Índices)

Cada integrante debe implementar su técnica dentro de la carpeta `src/indices/` siguiendo estas reglas críticas:

### 1. Regla de Oro: Acceso a Disco
**Está prohibido cargar archivos completos en memoria.** Debes usar el `BufferManager` instanciado en tu clase. Toda operación de lectura o escritura debe hacerse mediante:
- `self.buffer.read_page(id_pagina)`
- `self.buffer.write_page(id_pagina, datos_en_bytes)`

### 2. Formato de Salida
Todas las funciones (`add`, `search`, `remove`) deben devolver un diccionario con el formato definido en `base_index.py`:
```python
{
    "data": <resultado_de_la_operacion>,
    "disk_accesses": <total_de_lecturas_y_escrituras>,
    "execution_time_ms": <tiempo_en_milisegundos>
}
```

### 3. Flujo de Trabajo en Git
1. Crea una rama para tu técnica: `git checkout -b feature/nombre-tecnica`.
2. Trabaja únicamente en tu archivo dentro de `src/indices/`.
3. Haz `commit` y `push` de tu rama.
4. Abre un Pull Request para integrar tus cambios a `main`.

---

## 📝 Especificaciones Técnicas
- **Lenguaje:** Python 3.x
- **Tamaño de Página:** 4096 bytes (4 KB).
- **Persistencia:** Todos los datos deben guardarse en archivos binarios `.dat` dentro de la carpeta `data/`.

---

## 👥 Integrantes y Responsabilidades
- **Persona A:** Archivo Secuencial e Interfaz Gráfica.
- **Persona B:** Hash Extensible.
- **Persona C:** B+ Tree.
- **Persona D:** R-Tree (Datos Espaciales).
- **Persona E (Arquitecto):** SQL Parser, Buffer Manager y Coordinación.

---

### 💡 Tips para el equipo:
- No borren la carpeta `__init__.py` de las subcarpetas, es necesaria para que las importaciones funcionen.
- Si necesitan guardar objetos complejos, usen la librería `struct` para convertirlos a bytes antes de pasarlos al `BufferManager`.

---