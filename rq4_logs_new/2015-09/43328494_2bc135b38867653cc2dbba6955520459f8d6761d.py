import numpy as np
import matplotlib.pyplot as plt
from scipy import optimize as opt

t=np.linspace(0,10,1000);
A=1 #fijo
g=1 #fijo
m=1 #fijo
w=1
rest=0.5 #va de 0 a 1
y_0=0
v_0=2 #debe ser mayor que A*w


def floor(t):
    y_s=A*np.sin(w*t); #posición del suelo
    return y_s
def particle(t):
    y_p=y_0+(v_0*t)-((1/2.)*g*t**2); #posición de la partícula
    return y_p
def floorv(t):
    v_s=A*w*np.cos(w*t); #velocidad del suelo
    return v_s
def particlev(t):
    v_p=v_0-g*t #velocidad de la partícula
    return v_p
def resta(x):
    y=floor(x)-particle(x)
    return y
zero1=opt.bisect(resta,4,6);
for i in range():
    t_new=zero1
    v_p_new=(1+rest)*floorv(zero1)-rest*particlev(zero1) #velocidad de la particula despues de chocar por 1ra vez


plt.figure(1)
plt.clf()
plt.plot(t,floor(t),'r',t,particle(t),'b')
plt.xlabel('Tiempo [s]')
plt.ylabel('Altura [m]')
plt.axvline(zero1,color='g')
plt.show()