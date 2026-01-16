from flask import Flask, jsonify, request #Importamos módulos de flask
from Nomenclatura import Nomenclatura  # Importamos la clase que calcula la masa molar de un compuesto

def Problema(nivel):
    try:
        # Creamos un objeto de la clase Nomenclatura
        nomenclatura = Nomenclatura()
        iones = nomenclatura.Nivel(nivel)
        problema, id = nomenclatura.Problema(iones)

        return problema, id
    except ValueError as e:
        return str(e)

def ComprobarRespuesta(nivel, respuesta, pregunta, id):
    try:
        # Creamos un objeto de la clase Nomenclatura
        nomenclatura = Nomenclatura()
        respuestaCorrecta, comprobante = nomenclatura.ComprobarRespuesta(int(nivel), respuesta, pregunta, int(id))
        return respuestaCorrecta, comprobante
    except ValueError as e:
        return str(e)
