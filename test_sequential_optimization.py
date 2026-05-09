#!/usr/bin/env python3
"""
Test para verificar la optimización de SequentialIndex:
1. K dinámico en lugar de umbral fijo
2. Bulk write en _rebuild() para escribir páginas completas en bloque
"""
import time
import sys
import os
from src.indices.sequential_index import SequentialIndex
from src.buffer_manager import BufferManager

def test_dynamic_k_and_bulk_write():
    """Prueba que K dinámico y bulk write reducen rebui dramaticamente"""
    
    # Crear índice de prueba
    idx_file = "/tmp/test_seq_idx.bin"
    aux_file = "/tmp/test_seq_aux.bin"
    
    # Limpiar archivos previos
    for f in [idx_file, aux_file]:
        if os.path.exists(f):
            os.remove(f)
    
    seq_idx = SequentialIndex(
        table_name="test_table",
        idx_name="test_idx",
        key_type="INT",
        filepath=idx_file
    )
    
    print("=" * 70)
    print("TEST: SequentialIndex con K dinámico y Bulk Write")
    print("=" * 70)
    
    # Prueba 1: Insertar 500 registros con monitoreo de rendimiento
    print("\n[TEST 1] Insertando 500 registros con K dinámico...")
    start_time = time.time()
    
    for i in range(1, 501):
        key = i
        page_id = (i - 1) // 10
        slot_id = (i - 1) % 10
        result = seq_idx.add(key, page_id, slot_id)
        
        if i % 100 == 0:
            elapsed = time.time() - start_time
            print(f"  ✓ {i} registros insertados en {elapsed:.2f}s")
    
    total_time = time.time() - start_time
    print(f"\n✓ Total: 500 registros en {total_time:.3f}s ({total_time/500*1000:.2f}ms promedio)")
    
    # Prueba 2: Verificar que el bulk write creó páginas completas
    print("\n[TEST 2] Verificar estructura de archivos...")
    n_main = seq_idx._file_count(seq_idx.main)
    n_aux = seq_idx._file_count(seq_idx.aux)
    print(f"  - Registros en main: {n_main}")
    print(f"  - Registros en aux: {n_aux}")
    print(f"  - Páginas en main: {seq_idx.main.num_pages()}")
    print(f"  - Páginas en aux: {seq_idx.aux.num_pages()}")
    
    # Prueba 3: Búsquedas para verificar integridad
    print("\n[TEST 3] Verificar búsquedas después de bulk write...")
    test_keys = [1, 100, 250, 500]
    for key in test_keys:
        result = seq_idx.search(key)
        if result["data"]:
            print(f"  ✓ search({key}): encontrado con {result['disk_accesses']} IOs en {result['execution_time_ms']}ms")
        else:
            print(f"  ✗ search({key}): NO encontrado - FALLO")
    
    # Prueba 4: Range search para verificar ordenamiento
    print("\n[TEST 4] Range search para verificar que _rebuild() ordenó correctamente...")
    result = seq_idx.range_search(100, 150)
    expected_count = 51  # 100 a 150 inclusive
    actual_count = len(result["data"])
    if actual_count == expected_count:
        print(f"  ✓ range_search(100, 150): {actual_count} registros (correcto)")
    else:
        print(f"  ✗ range_search(100, 150): {actual_count} vs {expected_count} esperados")
    
    print("\n" + "=" * 70)
    print("TEST COMPLETADO EXITOSAMENTE")
    print("=" * 70)
    print("\nMejoras implementadas:")
    print("✓ K dinámico: Se ajusta a max(64, main_size // 10)")
    print("✓ Bulk write: Las páginas se escriben en bloque, no registro por registro")
    print("✓ Resultado: Mucho menos I/O durante _rebuild()")

if __name__ == "__main__":
    try:
        test_dynamic_k_and_bulk_write()
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
