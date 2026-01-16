# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 09:43:11 2023

@author: DELL
"""

#!/usr/bin/python
import os  #funciones para interactuar con el sistema operativo
#from Subfact_ina219 import INA219 # Librería desarrollada por Adafruit
from adafruit_ina219 import BusVoltageRange, INA219
import time
import board
from time import sleep
from datetime import datetime, timedelta

i2c_bus = board.I2C()  # utilizacion board.SCL y board.SDA
ina = INA219(i2c_bus)

#Datos
ic = 10000 #en mAh de la bateria bicicleta
st = 10 #intervalo de muestreo cada 
te=5 #tiempo (min) del experimento
variable = 1430.52
minutes = te/variable #calculo de la variable minutes

batterycapacity=ic # Capacidad inicial de energía
ec=0.0 # Consumo inicial de energía
currentlist=[0.0] # Vector de muestras de corriente
indice=1 # Inicializa vector de muestras
interval=2 #Intervalo (s) para el cálculo del consumo de energía
samplingtime=st # Intervalo de muestreo en (s)
M=3600/st # Número muestras a capturar en una hora
sampling=1 # Flag de activación/desactivación para la captura de muestras
starttest = datetime.now() #Tiempo de inicio de un experimento
endtest = starttest + timedelta(minutes) # Duración del experimento
fc= open("/MeasuresNodoX/EnergyConsumptionX.txt", "w") # Escritura del consumo
fc.write(" %.3f " % ec) # de energía en un fichero
fc.close()
while (sampling == 1): # Muestreo 
    currentlist.append(ina.getCurrent_mA())
    if datetime.now() > starttest + timedelta(seconds=interval):
        ec=ec+(currentlist[indice]/M)*100/ic # Cálculo del consumo (%)
        fc= open("MeasuresNodoX/EnergyConsumptionX.txt", "w") 
        fc.write(" %.3f " % ec) # Actualización del consumo
        fc.close()
        interval = interval+2 # Próximo intervalo de actualización
    if datetime.now() > endtest: # Escritura del conjunto de muestras
        fm= open("MeasuresNodoX/CurrentSamplesNodoX.txt", "w")
        for sample in currentlist:
            fm.write(str(sample))
            fm.write("\n")
        fm.close()
        sampling=0
    indice=indice+1
    sleep(st) # Intervalo de muestreo
os.system('halt') # Desactivación del nodo