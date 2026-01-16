import kivy
import json
from cv2 import cv2
import numpy as np
import os
from pathlib import Path

class Function:
    def __init__(self, nombre, funcion, group):
        self.nombre = nombre # nombre asignado de la funcion
        self.funcion = funcion # codigo cv2 para esa funcion
        self.group = group # grupo al que pertenece la funcion
    
    def evaluate(self, inputs):
        try:
            outputs = eval (self.funcion)(*inputs)
        except cv2.error:
            print(cv2.error)
        return outputs

def search_function(nombre):
    for element in funciones:
        if element.nombre == nombre:
            return element

funciones = []
# creación de todas las funciones disponibles
dir = str(Path(__file__).parent.absolute())
with os.scandir(dir + '\\funciones') as json_files:
    for element in json_files:
        json_file = open(element)
        data = json.load(json_file)
        for key in data:
            try:
                #BuscarFuncionJson(key['nombre'])
                fun = Function(key['nombre'], key['funcion'], key['group'])
                funciones.append(fun)
            except KeyError:
                pass


def orderby_groups():
    groups = {}
    for element in funciones:
        if element.group in groups:
            groups[element.group].append(element)
        else:
            groups[element.group] = [element]
    return groups

color = {
    "Input/Output" : (212/255, 56/255, 215/255, 1),
    "Geometry" : (1, 0, 139/255, 1),
    "Conversions" : (1, 3/255, 62/255, 1),
    "Local Operations" : (1, 69/255, 0, 1),
    "Point Operations" : (1, 131/255, 0, 1),
    "Arithmetic Operations" : (236/255, 227/255, 0, 1),
    "Numpy Functions" : (168/255, 244/255, 0, 1)
}
#verde 166/255, 209/255, 180/255, 1
#148/255, 211/255, 200/255, 1
#143/255, 210/255, 221/255, 1
#154/255, 206/255, 235/255, 1
#178/255, 200/255, 240/255, 1
#205/255, 193/255, 233/255, 1
#228/255, 187/255, 218/255, 1


#violeta 212/255, 56/255, 215/255, 1
#rosa 1, 0, 139/255, 1
#rojo 1, 3/255, 62/255, 1

#verde 84/255, 144/255, 0, 1