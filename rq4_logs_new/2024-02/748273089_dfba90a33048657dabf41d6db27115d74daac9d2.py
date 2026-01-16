import numpy as np
import matplotlib.pyplot as plt
import scipy as sp
import sys #system library
import os #libreria para navegar en consola

#a=sys.argv[1] #sys.argv es una lista de argumentos que pasan a python3 arg1 arg2 arg3

longitud=sys.argv[1] #asigna a la variable longitud el argumento 1 de python
name='I1_L_'+longitud+'nm_M1.txt' #creando un nombre estandar usando la variable longitud

os.chdir('data/') #entro a la carpeta "data"
data=np.loadtxt('variacion longitud de onda (1).txt',skiprows=5,max_rows=50) #lectura de archivo
os.chdir('..') #retorno al directorio principal

V=data[:,1] #segunda columna de data
I=data[:,2] #tercera columna de data

plt.plot(V,I,'o')
plt.show()