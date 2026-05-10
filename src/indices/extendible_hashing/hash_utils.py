import hashlib
import struct
from typing import Any

def generate_hash(key: Any) -> int:
    """
    Genera un valor Hash determinista de 32 bits para una llave dada.
    Usa MD5 internamente para asegurar una distribución uniforme (minimizar colisiones)
    y garantizar que el hash sea idéntico entre diferentes ejecuciones del programa.
    """
    # 1. Convertir la llave a su representación en bytes de forma segura
    if isinstance(key, int):
        # Entero de 4 bytes
        key_bytes = struct.pack('i', key)
    elif isinstance(key, float):
        # Float de 4 u 8 bytes (asumimos 4 para consistencia con tu config)
        key_bytes = struct.pack('f', key)
    elif isinstance(key, str):
        # String codificado a UTF-8
        key_bytes = key.encode('utf-8')
    else:
        # Fallback de seguridad
        key_bytes = str(key).encode('utf-8')

    # 2. Calcular MD5 (es suficientemente rápido para BD académicas y distribuye excelente)
    hash_digest = hashlib.md5(key_bytes).digest()

    # 3. Tomar solo los primeros 4 bytes del MD5 y convertirlos a un entero sin signo (32 bits)
    # '<I' significa Little-Endian, Unsigned Integer
    hash_int = struct.unpack('<I', hash_digest[:4])[0]
    
    return hash_int

def get_bits(hash_value: int, depth: int) -> int:
    """
    Extrae los 'depth' bits menos significativos (LSB) del valor hash.
    Estos bits se usan como índice (sufijo) para buscar en el Directorio.
    
    Ejemplo matemático:
    Si hash_value = 27 (En binario: 11011) y depth = 3:
    - Desplazamos 1 << 3 = 8 (Binario: 1000)
    - Restamos 1 = 7 (Binario: 0111) <- Esta es la máscara
    - 11011 & 0111 = 011 (Binario) = 3 (Decimal)
    """
    if depth == 0:
        return 0
    
    # Creamos una máscara de bits que tiene '1's en los últimos 'depth' espacios
    mask = (1 << depth) - 1
    
    # Aplicamos el operador AND bit a bit
    return hash_value & mask


# =====================================================================
# BLOQUE DE DEBUGGING Y PRUEBAS AISLADAS
# (Correr este archivo directamente con: python src/indices/extendible_hashing/hash_utils.py)
# =====================================================================
if __name__ == "__main__":
    print("--- TEST DE FUNCIONES DE HASH ---")
    
    # 1. Prueba de Determinismo
    k = 1045
    h1 = generate_hash(k)
    h2 = generate_hash(k)
    assert h1 == h2, "ERROR: La función hash no es determinista."
    print(f"Llave: {k} -> Hash de 32 bits: {h1}")
    
    # 2. Prueba de Extracción de bits
    # Elijamos un número binario fácil de leer: 43 -> 101011
    # Depth 1 (LSB 1 bit):   10101[1] -> 1
    # Depth 2 (LSB 2 bits): 1010[11] -> 3
    # Depth 3 (LSB 3 bits): 101[011] -> 3
    # Depth 4 (LSB 4 bits): 10[1011] -> 11
    test_hash = 43 
    print(f"\nProbando extracción sobre el hash {test_hash} (Binario: {bin(test_hash)}):")
    print(f"Depth 1: {get_bits(test_hash, 1)} (Esperado: 1)")
    print(f"Depth 2: {get_bits(test_hash, 2)} (Esperado: 3)")
    print(f"Depth 3: {get_bits(test_hash, 3)} (Esperado: 3)")
    print(f"Depth 4: {get_bits(test_hash, 4)} (Esperado: 11)")
    
    print("\n¡Todo funciona perfecto!")