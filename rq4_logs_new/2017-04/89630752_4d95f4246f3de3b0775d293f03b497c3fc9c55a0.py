"""
Receta para tirar las características básicas, y sus valores fraccionales
"""

#Tirar las características básicas

#Creamos un dado virtual
import random
def Dado (max, min = 1):
    """Calcula un valor aleatorio entre min y max. Normalmente es necesario
    sólo dar el valor máximo, pero también puedes dar el valor mínimo.
    Ejemplos: Dado(20), un dado de 20 caras; Dado(7,2), un dado entre 2 y 7"""
    return random.randint(min, max);

#Definimos una función para obtener la suma de 3 dados de 6 caras
def tirada_3D6():
    """Devuelve el valor suma de las 3 tiradas de 1 dado de 6 caras"""
    def lanza_dado():
        """devuelve la lista de las 3 tiradas del dado"""
        lanzamientos = [] #inicializamos una lista vacía
        for i in range(3): #iteramos en un bucle 3 veces
            lanzamientos.append(Dado(6)) #en cada iteración, lanzamos 1D6, y lo añadimos a la lista
        return lanzamientos  #devuelve una lista con 3 tiradas de D6
    return sum(i for i in lanza_dado())  #devuelve la suma de los elementos de la lista lanzamientos

#Primero, agrupamos las características en un tuple
características = ("Fue", "Des", "Con", "Int", "Sab", "Car")

#Ahora, los valores irán en una lista, que inicializamos a 0 valores, o lista vacía
valores = []

#Tiramos el D6 3 veces, una por cada característica
for caract in características:
    valores.append(tirada_3D6())

#Finalmente, creamos un diccionario clave:valor, en el que las claves son las características y los valores, los de la lista
características = dict((x, y) for (x, y) in zip(características, valores))