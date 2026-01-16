# Función para separar día, mes y año de cada registro y luego poder ordenarlo
def separar_fechas(linea):
    dia, mes, año = linea[2][:2], linea[2][2:4], linea[2][4:]
    return año, mes, dia, float(linea[5])

# Función recursiva para imprimir los nombres de cada columna
def imprimir_columnas(tupla):
    if len(tupla) > 0:
        print(tupla[0].center(19),end="")
        imprimir_columnas(tupla[1:])


# Función para imprimir la matriz
def imprimir_matriz(matriz, columnas):
    imprimir_columnas(columnas)
    print("\n" + "-" * 130)
    for fila in matriz:
        for elemento in fila:
            print(f"{elemento.center(19)}", end="")
        print()


# Programa Principal

criterio = input("Ingrese el paquete que desea buscar: ").lower()
print()

try:  # Controlamos excepciones
    paquetes = open(r"Paquetes.txt", 'rt', encoding='UTF-8')  # Abre el archivo
    matriz_paquetes = []  # Crea la lista vacía
    for linea in paquetes:
        if criterio in linea.lower():  # Convierte a minúsculas antes de comparar
            linea_separada = linea.split(";")  # Separa cada elemento del registro
            matriz_paquetes.append(linea_separada)  # Agrega cada registro a la matriz

    columnas = "Ciudad", "Región", "Fecha", "Cantidad Noches", "Transporte", "Precio", "Comidas" # Nombre de cada columna

    if len(matriz_paquetes) > 0:
        matriz_paquetes.sort(key=separar_fechas)  # Ordena la matriz por fecha y sino por precio
        print(f"Los Paquetes correspondientes a {criterio.capitalize()} son:")
        print()
        imprimir_matriz(matriz_paquetes, columnas)

    else:
        assert()

except FileNotFoundError:
    print("Archivo no encontrado")

except AssertionError:
    print(f"No se encontraron paquetes que coincidan con '{criterio.capitalize()}'")

except Exception as e:
    print(f"Error inesperado: {e}")

finally:
    try:
        paquetes.close()
    except NameError:
        pass