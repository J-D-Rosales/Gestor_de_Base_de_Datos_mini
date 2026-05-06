import os
import shutil
from src.indices.extendible_hashing import ExtendibleHashing

def limpiar_datos_prueba():
    """Limpia la carpeta de datos de prueba antes de empezar para tener un entorno en blanco."""
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data")

def test_hash_enteros():
    print("--- INICIANDO TEST: HASH CON ENTEROS (INT) ---")
    # Instanciamos la tabla "empleados" con llaves tipo INT
    idx = ExtendibleHashing(table_name="empleados", index_name="emp_hash", key_type="INT")

    # 1. Prueba de ADD
    print("1. Probando ADD...")
    idx.add(10, 1000) # Key: 10, Value: 1000
    idx.add(25, 2500)
    idx.add(13, 1300)
    
    # 2. Prueba de SEARCH (Positiva)
    print("2. Probando SEARCH (Existente)...")
    res1 = idx.search(25)
    assert res1["data"] == 2500, f"Error: Se esperaba 2500, pero se obtuvo {res1['data']}"
    print(f"   [OK] Llave 25 encontrada correctamente. Costo I/O: {res1['disk_accesses']}")

    # 3. Prueba de REMOVE
    print("3. Probando REMOVE...")
    idx.remove(25)
    
    # 4. Prueba de SEARCH (Negativa - Después de borrar)
    print("4. Probando SEARCH (Eliminado)...")
    res2 = idx.search(25)
    assert res2["data"] is None or res2["data"] is False, "Error: La llave 25 sigue existiendo después del remove!"
    print("   [OK] Llave 25 fue eliminada exitosamente.")

def test_hash_strings():
    print("\n--- INICIANDO TEST: HASH CON STRINGS (VARCHAR) ---")
    # Instanciamos la tabla "ciudades" con llaves tipo VARCHAR(20)
    idx = ExtendibleHashing(table_name="ciudades", index_name="ciu_hash", key_type="VARCHAR", key_size=20)

    print("1. Probando ADD Polimórfico...")
    idx.add("Lima", 51)
    idx.add("Arequipa", 54)
    idx.add("Cusco", 84)

    print("2. Probando SEARCH de Strings...")
    res1 = idx.search("Arequipa")
    assert res1["data"] == 54, f"Error: Se esperaba 54, pero se obtuvo {res1['data']}"
    print(f"   [OK] String 'Arequipa' encontrado correctamente. Costo I/O: {res1['disk_accesses']}")

    print("3. Probando REMOVE de Strings...")
    idx.remove("Arequipa")
    res2 = idx.search("Arequipa")
    assert res2["data"] is None or res2["data"] is False, "Error: El string 'Arequipa' no fue borrado correctamente."
    print("   [OK] String 'Arequipa' fue eliminado exitosamente.")

def test_saturacion_y_split():
    print("\n--- INICIANDO TEST: SATURACIÓN Y SPLIT ---")
    # Usamos un key_size artificialmente inmenso para que en una página (4KB) entren muy pocos registros
    # Esto forzará múltiples Splits y Doublings del directorio rápidamente.
    idx = ExtendibleHashing(table_name="test", index_name="split_hash", key_type="VARCHAR", key_size=1000)
    
    print(f"   Capacidad máxima por bucket: {idx.max_records} registros.")
    
    for i in range(20):
        idx.add(f"Clave_{i}", i)
        
    # Verificamos que los datos sigan ahí a pesar de que el archivo se partió múltiples veces
    res = idx.search("Clave_15")
    assert res["data"] == 15, "Error: Pérdida de datos durante el Split/Directory Doubling."
    print(f"   [OK] Se insertaron 20 registros forzando splits. Los datos se remapearon sin pérdida.")
    print(f"   Profundidad global alcanzada: {idx.global_depth}")

if __name__ == "__main__":
    limpiar_datos_prueba()
    try:
        test_hash_enteros()
        test_hash_strings()
        test_saturacion_y_split()
        print("\n✅ ¡TODAS LAS PRUEBAS FUERON EXITOSAS! EL MÓDULO ESTÁ LISTO PARA ACOPLARSE.")
    except AssertionError as e:
        print(f"\n❌ FALLO EN LA PRUEBA: {e}")