from src.parser.sql_parser import SQLParser

def main():
    print("==================================================")
    print("  MINI SGBD - PROYECTO 1 (FASE 1) ")
    print("==================================================")
    print("Escribe 'EXIT' para salir.\n")

    parser = SQLParser()

    while True:
        try:
            query = input("SQL> ")
            if query.strip().upper() == "EXIT":
                print("Cerrando el motor de base de datos...")
                break
                
            if not query.strip():
                continue

            # Ejecutar la consulta a través del parser
            resultado = parser.execute_query(query)
            
            # Imprimir el resultado formateado
            print("--- Resultado ---")
            for key, value in resultado.items():
                print(f"  {key}: {value}")
            print("-----------------\n")

        except KeyboardInterrupt:
            print("\nCerrando...")
            break
        except Exception as e:
            print(f"\n[ERROR CRÍTICO]: {e}\n")

if __name__ == "__main__":
    main()