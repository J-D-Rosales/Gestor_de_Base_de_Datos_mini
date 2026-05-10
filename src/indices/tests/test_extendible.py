import os
import shutil
# Importamos la clase principal que ya contiene todo orquestado
from extendible_hashing import ExtendibleHashing
def setup_clean_env():
    """Limpia archivos de pruebas anteriores para un inicio fresco."""
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)

def run_automated_tests():
    setup_clean_env()
    print("Iniciando Pruebas de Hash Extensible...\n")

    # 1. Inicializamos el índice con un tamaño de página muy pequeño
    # Usamos 128 bytes para forzar que los buckets se llenen rápido y haya Splits constantes
    index = ExtendibleHashing(
        table_name="test_table", 
        index_name="test_idx", 
        key_type="INT", 
        page_size=128 
    )

    # 2. Inserción Masiva
    # Insertamos 50 registros secuenciales. Como la página es de 128 bytes, 
    # cabrán muy pocos registros por bucket, forzando estrés en el directorio.
    print("[1] Insertando 50 registros para forzar splits en cascada...")
    for i in range(1, 51):
        # Simulemos page_id = i*10, slot_id = i
        index.add(key=i, page_id_value=i*10, slot_id_value=i)

    # 3. Inspección Visual
    print("\n[2] Volcado de estado (Inspección Visual):")
    index.print_directory_state()
    
    # Imprimimos los primeros 3 buckets físicos para ver cómo quedaron repartidos
    unique_pages = list(set(index.directory.bucket_pointers))
    for pid in unique_pages[:3]:
        index.print_bucket_state(pid)

    # 4. Prueba Crítica: Validación de Invariantes Matemáticas
    print("\n[3] Ejecutando Validación Estricta de Invariantes...")
    index.validate_index()  # Si no colapsa aquí, el algoritmo base es 100% sólido

    # 5. Prueba de Búsqueda
    print("\n[4] Validando Búsquedas de llaves existentes...")
    search_res = index.search(key=25)
    # Verificamos que el resultado no esté vacío y los datos coincidan
    assert len(search_res["data"]) > 0, "Error: No se encontró la llave 25"
    assert search_res["data"][0] == (250, 25), "Error: Datos incorrectos para la llave 25"
    print(f"[OK] Búsqueda exitosa. IO Cost: {search_res['disk_accesses']} lectura(s)")

    print("\n[5] Validando Búsqueda de llave inexistente...")
    search_res_null = index.search(key=999)
    assert len(search_res_null["data"]) == 0, "Error: Encontró una llave que no existe"
    print(f"[OK] Búsqueda nula correcta. IO Cost: {search_res_null['disk_accesses']} lectura(s)")

    print("\n=== TODAS LAS PRUEBAS AUTOMATIZADAS PASARON CON ÉXITO ===")

if __name__ == "__main__":
    run_automated_tests()