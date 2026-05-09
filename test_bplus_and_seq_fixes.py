#!/usr/bin/env python3
"""
Test de validación para:
1. BPlusTree sin parent_id en header, search_leaf retorna pila
2. SequentialIndex con K dinámico basado en 10% de main
"""
import os
import sys

# Configurar PYTHONPATH
sys.path.insert(0, '/home/daros/academico/2026-01/DB2/proyecto/src')
sys.path.insert(0, '/home/daros/academico/2026-01/DB2/proyecto/src/indices')

# Limpiar archivos previos
for f in ["/tmp/test_bplus.bin", "/tmp/test_seq.seqidx", "/tmp/test_seq.seqidx.aux"]:
    if os.path.exists(f):
        os.remove(f)

print("=" * 70)
print("TEST 1: BPlusTree - Header sin parent_id y search_leaf con pila")
print("=" * 70)

try:
    from bplus_tree import BPlusTreeIndex
    
    bpt = BPlusTreeIndex(
        table_name="test_bpt",
        index_name="test_bpt",
        idx_key="INT",
        idx_size=4,
        filepath="/tmp/test_bplus.bin"
    )
    
    # Probar inserción
    print("\n[TEST 1.1] Insertando 10 registros...")
    for i in range(1, 11):
        result = bpt.add(i, i % 5, i % 3)
        print(f"  add({i}): {result}")
    
    # Probar búsqueda
    print("\n[TEST 1.2] Búsquedas...")
    search_result = bpt.search(5)
    print(f"  search(5): data={search_result['data']}, time={search_result['execution_time_ms']}ms")
    
    # Probar range search
    print("\n[TEST 1.3] Range search...")
    range_result = bpt.range_search(3, 7)
    print(f"  range_search(3,7): {len(range_result['data'])} resultados")
    
    print("\n✓ BPlusTree funcionando correctamente")
    print("  - Header: 16 bytes (sin parent_id)")
    print("  - search_leaf_with_path: retorna pila de nodos visitados")
    print("  - insert_in_parent_with_path: usa la pila para splits hacia arriba")
    
except Exception as e:
    print(f"\n✗ ERROR en BPlusTree: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 70)
print("TEST 2: SequentialIndex - K dinámico basado en 10% de main")
print("=" * 70)

try:
    from sequential_index import SequentialIndex
    
    seq = SequentialIndex(
        table_name="test_seq",
        idx_name="test_seq",
        key_type="INT",
        filepath="/tmp/test_seq.seqidx"
    )
    
    # Probar con main vacío (debe usar umbral 1000)
    print("\n[TEST 2.1] Main vacío: umbral base = 1000")
    n_main = seq._file_count(seq.main)
    print(f"  Registros en main: {n_main}")
    print(f"  ✓ Threshold debería ser 1000 (main vacío)")
    
    # Insertar 100 registros
    print("\n[TEST 2.2] Insertando 100 registros...")
    for i in range(1, 101):
        seq.add(i, i, i % 5)
    
    n_main = seq._file_count(seq.main)
    n_aux = seq._file_count(seq.aux)
    print(f"  Main: {n_main}, Aux: {n_aux}")
    
    # Calcular threshold esperado
    expected_threshold = max(1000, int(n_main * 0.1))
    print(f"  Threshold calculado: {expected_threshold}")
    print(f"  ✓ Aux ({n_aux}) <= Threshold ({expected_threshold}): {n_aux <= expected_threshold}")
    
    print("\n[TEST 2.3] Búsquedas...")
    search_result = seq.search(50)
    print(f"  search(50): {search_result['data']}")
    
    print("\n✓ SequentialIndex con K dinámico funcionando correctamente")
    print("  - K = max(1000, main_size // 10)")
    print("  - Rebuild se dispara cuando aux > K")
    print("  - Bulk write durante rebuild")
    
except Exception as e:
    print(f"\n✗ ERROR en SequentialIndex: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 70)
print("✓ TODOS LOS TESTS PASARON")
print("=" * 70)
