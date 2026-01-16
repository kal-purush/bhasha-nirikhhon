#! /usr/bin/python

"""
Este script crea la clase Personaje, que tendrá los atributos
y características básicas comunes a todos los personajes
"""

class Personaje(object):
    """Este script crea la clase Personaje, que tendrá los atributos
    y características básicas comunes a todos los personajes."""
    def __init__(self, nombre, clase, nivel, alineamiento, raza, sexo, edad, altura, peso, pelo, ojos, diox, manobuena, jugador, fue, des, con, int, sab, car, asp, hon):
        super(Personaje, self).__init__()
        self.nombre = ""
        self.clase = ""
        self.nivel = 1
        self.alineamiento = ""
        self.raza = ""
        self.sexo = ""
        self.edad = 0
        self.altura = 0
        self.peso = 0
        self.pelo = ""
        self.ojos = ""
        self.diox = ""
        self.manobuena = ""
        self.jugador = ""
        self.fue = 0
        self.des = 0
        self.con = 0
        self.int = 0
        self.sab = 0
        self.car = 0
        self.asp = 0
        self.hon = 0