import numpy as np
from matplotlib import pyplot as plt
from scipy.special import erf #importa la funcion error que va a ser usada

#trabajo con el archivo de DATA2
Texp = np.loadtxt('datos3.txt', delimiter = ",") #cargo la matriz de temperatura medida en loS 6 canales
tiempo = np.loadtxt('tiempos3.txt', delimiter = ",") #cargo la matriz de tiempo

#estos son los parametros desconocidos a inferir
F0 = 50
K = 100
k = 0.1

#defino la funcion T(x,t) teorica de la guia
def modelo(x, t):
	tita = (k * t / np.pi) ** 0.5 * np.exp(-x**2 / (4*k*t))
	tita -= (x / 2 * erf(x / 2* (k*t) ** 0.5))
	tita = tita * 2 * F0 / K + 31 # le sumo la temperatura ambiente 31 gradoC
	return tita

#defino las coordenadas de cada canal en metros, CHEQUEAR PORQUE NO SON ESTAS
xchannels = np.array([32.98,24.96,21.19,16.40,12.31,8.14])
#T teorica va a tener las mismas dimensiones que la T experimental
#Se define como una matriz en blanco
Tteo = np.zeros((6, len(tiempo[0,:])))
#Genero el loop para definir sus valores en base a la funcion ya definida
i = 0
for x in xchannels:
    j = 0
    for t in tiempo[0,:]:
        Tteo[i,j] = modelo(x,t) #modelo(x,t)
        j += 1
    i += 1

#ploteo solo a modo de ejemplo para comparar
#plt.plot(tiempo[0,:],Texp[1,:],'b.', label = 'CH1', linewidth=0.5)
for j in range(6):
    plt.loglog(tiempo[j,:] ** 0.5, Texp[j,:] - Texp[j,0],'g.', linewidth=0.5)
plt.loglog(tiempo, tiempo ** 0.5, 'b.')
plt.grid(True) # Para que quede en hoja cuadriculada
plt.title('LogLog: Temperatura en funcion del tiempo')
plt.xlabel('Tiempo (s)')
plt.ylabel('Temperatura (C)')
plt.legend(loc = 'best')
plt.show()