import csv

# Nombre del archivo CSV
nombre_archivo = 'datosperfil.csv'

# Abrir el archivo en modo escritura
with open(nombre_archivo, mode='w', newline='') as archivo_csv:
    escritor_csv = csv.writer(archivo_csv)

    # Escribir el encabezado
    escritor_csv.writerow(['Nombre', 'Carnet', 'Año que cursa', 'Curso'])
    #"Nombre:\nCarnet:\nAño que cursa:\nQuímica 1 o 2:"

    # Inicializar una variable para controlar el ciclo
    continuar = True

    while continuar:
        # Solicitar al usuario que ingrese datos
        nombre = input("Ingrese el nombre: ")
        
        if nombre.lower() == 'salir':
            continuar = False
            continue  # No se escribe nada y se continúa al siguiente ciclo

        carnet = input("Ingrese su carnet: ")
        AnioCursa = input("Ingrese el año que cursa: ")
        curso = input("Ingrese el curso en el que se encuentra (Química 1 o 2)")

        # Escribir los datos en el archivo
        escritor_csv.writerow([nombre, carnet, AnioCursa, curso])
        print(f"Datos de {nombre} registrados.")

print(f"Los datos se han escrito en {nombre_archivo}.")
