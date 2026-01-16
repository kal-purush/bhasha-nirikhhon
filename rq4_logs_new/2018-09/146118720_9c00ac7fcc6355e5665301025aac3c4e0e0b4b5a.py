#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 09:09:15 2018

@author: Lucas
"""

import numpy as np
import matplotlib.pyplot as plt


# %%

data = np.genfromtxt('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/IV_diodo.dat',delimiter=',',skip_header=1,usecols = (1, 4, 5))

tiempo = data[:,0]
corriente_diodo = data[:,1]
voltaje_diodo = data[:,2]

fig = plt.figure(1)
plt.plot(tiempo,voltaje_diodo)
plt.xlabel('Tiempo (s)')
plt.ylabel('Caída de tensión en el diodo (Vsoft)')

fig = plt.figure(2)
plt.plot(voltaje_diodo,corriente_diodo)
plt.xlabel('Voltaje (Vsoft)')
plt.ylabel('Corriente (Vsoft / $\Omega$)')
#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/curva_iv_diodo.png')

#%% caracterización del sistema (entrada-salida)

data = np.genfromtxt('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/barrido6y7.dat',delimiter=',',skip_header=1,usecols = (1, 2, 3, 4))

frecuencia = data[:, 0]
amplitud_enviada = data[:, 1]
amplitud_leida = data[:, 2]

amplitud_leida_1_enviada = amplitud_leida[0:len(amplitud_leida):7]
amplitud_leida_5_enviada = amplitud_leida[1:len(amplitud_leida):7]
amplitud_leida_10_enviada = amplitud_leida[2:len(amplitud_leida):7]
amplitud_leida_15_enviada = amplitud_leida[3:len(amplitud_leida):7]
amplitud_leida_20_enviada = amplitud_leida[4:len(amplitud_leida):7]
amplitud_leida_25_enviada = amplitud_leida[5:len(amplitud_leida):7]
amplitud_leida_30_enviada = amplitud_leida[6:len(amplitud_leida):7]
frecuencias = frecuencia[0:len(amplitud_leida):7] # para todos los casos son las mismas frecuencias

fig = plt.figure(1)
plt.scatter(frecuencias,amplitud_leida_1_enviada/0.1)
plt.scatter(frecuencias,amplitud_leida_5_enviada/0.5)
plt.scatter(frecuencias,amplitud_leida_10_enviada/1)
plt.scatter(frecuencias,amplitud_leida_15_enviada/1.5)
plt.scatter(frecuencias,amplitud_leida_20_enviada/2)
plt.scatter(frecuencias,amplitud_leida_25_enviada/2.5)
plt.scatter(frecuencias,amplitud_leida_30_enviada/3)
plt.legend(['0.1','0.5','1.0','1.5','2.0','2.5','3.0'])

plt.xlabel('Frecuencia (Hz)')
plt.ylabel('Amplitud medida / Amplitud enviada')
fig.tight_layout()

#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/cociente_amplitudes_medida_y_enviada.png')

fig = plt.figure(2)
plt.scatter(frecuencias,amplitud_leida_1_enviada)
plt.scatter(frecuencias,amplitud_leida_5_enviada)
plt.scatter(frecuencias,amplitud_leida_10_enviada)
plt.scatter(frecuencias,amplitud_leida_15_enviada)
plt.scatter(frecuencias,amplitud_leida_20_enviada)
plt.scatter(frecuencias,amplitud_leida_25_enviada)
plt.scatter(frecuencias,amplitud_leida_30_enviada)
plt.legend(['0.1','0.5','1.0','1.5','2.0','2.5','3.0'])

plt.xlabel('Frecuencia (Hz)')
plt.ylabel('Amplitud medida (Vsoft)')
fig.tight_layout()

#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/amplitud_medida_volrecording7.png')

data = np.genfromtxt('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/barrido9.dat',delimiter=',',skip_header=1,usecols = (1, 2, 3, 4))
frecuencia = data[:, 0]
amplitud_enviada = data[:, 1]
amplitud_leida = data[:, 2]

amplitud_leida_1_enviada = amplitud_leida[0:len(amplitud_leida):7]
amplitud_leida_5_enviada = amplitud_leida[1:len(amplitud_leida):7]
amplitud_leida_10_enviada = amplitud_leida[2:len(amplitud_leida):7]
amplitud_leida_15_enviada = amplitud_leida[3:len(amplitud_leida):7]
amplitud_leida_20_enviada = amplitud_leida[4:len(amplitud_leida):7]
amplitud_leida_25_enviada = amplitud_leida[5:len(amplitud_leida):7]
amplitud_leida_30_enviada = amplitud_leida[6:len(amplitud_leida):7]
frecuencias = frecuencia[0:len(amplitud_leida):7] # para todos los casos son las mismas frecuencias

fig = plt.figure(3)
plt.scatter(frecuencias,amplitud_leida_1_enviada/0.1)
plt.scatter(frecuencias,amplitud_leida_5_enviada/0.5)
plt.scatter(frecuencias,amplitud_leida_10_enviada/1)
plt.scatter(frecuencias,amplitud_leida_15_enviada/1.5)
plt.scatter(frecuencias,amplitud_leida_20_enviada/2)
plt.scatter(frecuencias,amplitud_leida_25_enviada/2.5)
plt.scatter(frecuencias,amplitud_leida_30_enviada/3)
plt.legend(['0.1','0.5','1.0','1.5','2.0','2.5','3.0'])

plt.xlabel('Frecuencia (Hz)')
plt.ylabel('Amplitud medida / Amplitud enviada')
fig.tight_layout()

#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/cociente_amplitudes_medida_y_enviada_volrecording100.png')

fig = plt.figure(4)
plt.scatter(frecuencias,amplitud_leida_1_enviada)
plt.scatter(frecuencias,amplitud_leida_5_enviada)
plt.scatter(frecuencias,amplitud_leida_10_enviada)
plt.scatter(frecuencias,amplitud_leida_15_enviada)
plt.scatter(frecuencias,amplitud_leida_20_enviada)
plt.scatter(frecuencias,amplitud_leida_25_enviada)
plt.scatter(frecuencias,amplitud_leida_30_enviada)
plt.legend(['0.1','0.5','1.0','1.5','2.0','2.5','3.0'])

plt.xlabel('Frecuencia (Hz)')
plt.ylabel('Amplitud medida (Vsoft)')
fig.tight_layout()

#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/amplitud_medida_volrecording100.png')
# %% curva de calibracion de la placa de audio al osciloscopio

# levanto barrido_condiciones_diodo porque barrido_calibracion no está (quizás no lo guardamos). Las frecuencias no serán exactamente las mismas.

data = np.genfromtxt('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/barrido_condiciones_diodo.dat',delimiter=',',skip_header=1,usecols = (1, 2, 3, 4))

frecuencia = data[:, 0]
amplitud_enviada = data[:, 1]
amplitud_leida = data[:, 2]

color = 'tab:blue'

fig, ax1 = plt.subplots()
ax1.scatter(frecuencia, amplitud_leida, color=color)
ax1.set_ylabel('Amplitud leida (Vsoft)',color=color)
ax1.tick_params(axis='y', labelcolor = color)

# ahora los valores tomados a mano leyendo el osciloscopio

frecuencias = [1000,10,100,10000,20000,25000,17000,15000,22000]
frecuencias = np.sort(frecuencias)
voltajes_pp = [2.6,2.6,2.56,2.58,1.88,0.18,2.54,2.60,1.10] # medidos en el osciloscopio
voltajes_pp = [2.6,2.56,2.56,2.58,2.60,2.54,1.88,1.10,0.18]
errores_voltajes_pp = [0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.01] ;

ax2 = ax1.twinx()
color = 'tab:red'
ax2.scatter(frecuencias,voltajes_pp, color=color)
ax2.set_xlabel('Frecuencia (Hz)')
ax2.set_ylabel('Voltaje pico a pico (V)', color=color)
ax2.tick_params(axis='y', labelcolor = color)

fig.tight_layout()

#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/amplitud_medida_en_pc_y_osciloscopio_vs_freq.png')

#%% del generador de funciones a la placa de audio
frecuencias = [1, 10, 15000, 18000, 20, 20000, 22000, 30000, 40000, 50, 100, 200] # en Hz, las escribo a mano (tomé solamene los casos de 300mVpp, y no tomé el de continua -100 uHz)
voltajes = [300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300] # en mVpp, también a mano
data = np.genfromtxt('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/Prueba input/200Hz300mVpp.dat')
tiempo = data[:, 0]
voltaje = data[:, 1] # en unidades del software
porcentaje_a_recortar = 10 # a cada lado
tiempo = tiempo[int(len(tiempo)*porcentaje_a_recortar/100):int(len(tiempo)*(1-porcentaje_a_recortar/100))]
voltaje = voltaje[int(len(voltaje) * porcentaje_a_recortar/100):int(len(voltaje)*(1 - porcentaje_a_recortar/100))]

vpps = [2392, 4740, 4757, 4416, 4742, 3469, 1853, 12, 13, 4744, 4751, 4739]
vpp = max(voltaje) - min(voltaje)

fig = plt.figure(1)
plt.plot(tiempo,voltaje)
plt.xlabel('Tiempo (s)')
plt.ylabel('Voltaje (Vsoft)')
plt.tight_layout()
#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/Prueba input/200Hz300mVpp_Vsoft_vs_t.png')

fig = plt.figure(2)
plt.scatter(frecuencias,vpps)
plt.xlabel('Frecuencia (Hz)')
plt.ylabel('Voltaje pico a pico (Vsoft)')
plt.xscale('log')
plt.tight_layout()
#fig.savefig('/Doctorado/Materias/Instrumentacion/instrumentacionycontrol/Prueba input/Vpps_vs_frecuencias_xlog.png')

# en 200 Hz, comparando 100 y 300 mVpp

V_medido_200Hz100mVpp = 1588
V_medido_200Hz300mVpp = 4739