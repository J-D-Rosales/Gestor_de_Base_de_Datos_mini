"""
Generador de datos de prueba para el índice B+ Tree.

Edita las listas INT_KEYS y STR_KEYS de abajo con los valores que quieras
insertar (en el orden de inserción). Luego ejecuta:

    python tests/generate_test_data.py

Genera dos archivos binarios consumidos por el __main__ de bplus_tree.py:

    tests/data/records_int.bin   -> registros <iii>      (key, page_id, slot_id)
    tests/data/records_str.bin   -> registros <{N}sii>   (key_bytes, page_id, slot_id)
"""

import os
import struct

# =====================================================================
# EDITAR ESTAS LISTAS — los valores se insertarán en este orden
# =====================================================================

# 9 enteros elegidos para forzar splits con orden=3
INT_KEYS = [50, 30, 70, 20, 40, 60, 80, 10, 90]

# 9 strings (tamaño de llave = STR_SIZE)
STR_KEYS = ["mar", "abc", "sol", "luz", "paz", "fin", "voz", "rey", "uno"]
STR_SIZE = 8

# =====================================================================

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data")
os.makedirs(OUT_DIR, exist_ok=True)


def _rid(i):
    """Asigna un RID sintético (page_id, slot_id) a cada registro."""
    return i // 3, i % 3


def write_int_records(keys, path):
    with open(path, "wb") as f:
        for i, k in enumerate(keys):
            page_id, slot_id = _rid(i)
            f.write(struct.pack('<iii', k, page_id, slot_id))
    print(f"  [OK] {len(keys)} registros INT -> {path}")


def write_str_records(keys, size, path):
    fmt = f'<{size}sii'
    with open(path, "wb") as f:
        for i, k in enumerate(keys):
            page_id, slot_id = _rid(i)
            kb = k.encode('utf-8')[:size].ljust(size, b'\x00')
            f.write(struct.pack(fmt, kb, page_id, slot_id))
    print(f"  [OK] {len(keys)} registros STR (size={size}) -> {path}")


if __name__ == "__main__":
    print("Generando datos de prueba para B+ Tree...")
    write_int_records(INT_KEYS, os.path.join(OUT_DIR, "records_int.bin"))
    write_str_records(STR_KEYS, STR_SIZE, os.path.join(OUT_DIR, "records_str.bin"))
    print("\nListo. Ahora ejecuta:")
    print("    python src/indices/bplus_tree.py")
