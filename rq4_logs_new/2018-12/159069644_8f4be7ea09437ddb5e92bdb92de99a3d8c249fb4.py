import numpy as np
import matplotlib.pyplot as plt

data=np.loadtxt("burgers.dat")
##Divide los datos del archivo burgers.dat en dos vectores iguales
x=np.split(data,2)
plt.figure()
plt.plot(np.linspace(0,4,num=401),x[0])
plt.plot(np.linspace(0,4,num=401),x[1])
plt.savefig("burgers.pdf")