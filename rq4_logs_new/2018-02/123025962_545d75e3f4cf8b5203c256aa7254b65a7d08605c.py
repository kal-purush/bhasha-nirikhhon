"""
    Autor: Carlos Almario
    Fecha: febrero 27 2018
    Archivo read_file.py, Contiene una función para leer un archivo txt e insertarlos en una matriz
"""

matriz = open('matriz.txt')
world = []

""" Crea la matriz con base a los datos del archivo txt """
for line_matriz in matriz:
    split_line_matriz = line_matriz.strip().split(" ")
    world.append(split_line_matriz)


""" Imprime la matriz en consola"""
for row in world:
    print(row,'\n')

