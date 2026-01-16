import numpy as np
from matplotlib import pyplot as plt
from scipy.special import erf #importa la funcion error que va a ser usada

#importo los datos depurados
tiempo = np.load('tiempoTransitorio.npy')
Texp = np.load('TempTransitorio.npy')
#ploteo para cada canal, el cambio de la temperatura con respecto a su valor inicial
#esto justamente representa la funcion tita de la guia
for j in range(6):
    plt.loglog(tiempo[j,:], Texp[j,:] - Texp[j,0],'g.', linewidth=0.5)
plt.loglog(tiempo, tiempo ** 0.5, 'b.') #si le pongo un label, me etiqueta cada punto!
plt.grid(True) # Para que quede en hoja cuadriculada
plt.title('LogLog: Tita en funcion del tiempo')
plt.xlabel('Tiempo (s)')
plt.ylabel('Temperatura (C)')
plt.legend(loc = 'best')
plt.show()