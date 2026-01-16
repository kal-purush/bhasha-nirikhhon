# -*- coding: utf-8 -*-


from pylab import *

#Constante de Newton [unité SI]
G = 6e-11
#célérité lumière dans le vide [m/s]
c= 3e8

#Masses des objets [kg]
m1 = 2e29
m2 = 2e30

#fréquence max émise par le soleil (en Hz)
f = 0.6e6

#Positions initiales [m]
x1_0 = 15e10
y1_0 = 0

x2_0 = 1
y2_0 = 1

#Vitesses initiales [m/s]

v1x_0 = 0
v1y_0 = 3e4

v2x_0 = 0
v2y_0 = 0

#Paramètres de la simulation

T = 30e7 #Durée de la simulation :10 ans [s]
dt = T/100000 #Intervale de temps

#Initialisation

x1 = zeros(int(T/dt))
y1 = zeros(int(T/dt))
x2 = zeros(int(T/dt))
y2 = zeros(int(T/dt))
vx1 = zeros(int(T/dt))
vy1 = zeros(int(T/dt))
vx2 = zeros(int(T/dt))
vy2 = zeros(int(T/dt))
frecu = zeros(int(T/dt))

x1[0] = x1_0
y1[0] = y1_0
x2[0] = x2_0
y2[0] = y2_0
vx1[0] = v1x_0
vy1[0] = v1y_0
vx2[0] = v2x_0
vy2[0] = v2y_0
frecu[0] = f

t = zeros(int(T/dt))
t[0] = 0
t[1] = dt

x1[1] = vx1[0]*dt + x1[0]
y1[1] = vy1[0]*dt + y1[0]
x2[1] = vx2[0]*dt + x2[0]
y2[1] = vy2[0]*dt + y2[0]



for i in range (2,int(T/dt)) : 
    t[i] = i*dt
    R2 = (x1[i-1]-x2[i-1])**2+(y1[i-1]-y2[i-1])**2
    R = sqrt(R2)
    Rx = x1[i-1]-x2[i-1]
    Ry = y1[i-1]-y2[i-1]
    
    x1[i] = -G*m2*(dt**2)*Rx/(R2*R) + 2*x1[i-1] - x1[i-2]
    x2[i] = -G*m1*(dt**2)*(-Rx)/(R2*R) + 2*x2[i-1] - x2[i-2]
    y1[i] = -G*m2*(dt**2)*Ry/(R2*R) + 2*y1[i-1] - y1[i-2]
    y2[i] = -G*m1*(dt**2)*(-Ry)/(R2*R) + 2*y2[i-1] - y2[i-2]
    vx1[i] = (x1[i]+x1[i-1])/dt
    vx2[i] = (x2[i] - x2[i - 1]) / dt
    vy2[i] = (y2[i] - y2[i - 1]) / dt
    frecu[i] = (c/(c-vx2[i]))*f
    frecu[1]=frecu[2]
    #print(frecu[i])
    
#plot(x1,y1,'b')
#plot(x2,y2,'r')
#axis("equal")
#plot(t,frecu)
plot(t,vx2)
fig, axs = plt.subplots(2, 1)
axs[0].plot(x1, y1,'b')
axs[0].plot(x2, y2,'r')
axs[0].axis("equal")
axs[0].set(ylabel='y [m]', xlabel='x [m] ')
axs[0].set_title("Trajectoires des astres")
#axs[0, 1].plot(t, vx2, 'tab:orange')
#axs[0, 1].set_title("Vitesse étoile")
#axs[0, 1].set(xlabel='vitesse [m/s]', ylabel='t [s]')
axs[1].plot(t, frecu, 'tab:green')
axs[1].set_title("fréquence reçu")
axs[1].set(ylabel='Fréquence [Hz]', xlabel='temps [s]')

show()