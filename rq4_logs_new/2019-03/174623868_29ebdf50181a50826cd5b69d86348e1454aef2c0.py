#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 12:34:09 2019

@author: ep-m2-07
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 09:44:26 2019

@author: Galileo Cappella
"""

import random
import numpy as np

def random_start(n, x_size, y_size, z_size): #A: s= side, d= deer, l= lions
    r= np.repeat(" ", x_size*y_size*z_size).reshape(x_size, y_size, z_size)
    x_pos= []
    y_pos= []
    z_pos= []
    
    i= 0
    while i < n:
        pos= (random.randint(0, x_size-1), random.randint(0, y_size-1), random.randint(0, z_size-1))
        if r[pos] == " ":
            r[pos]= "P"
            i+= 1
            x_pos.append(pos[0])
            y_pos.append(pos[1])
            z_pos.append(pos[2])

    print('Posiciones iniciales = \n', r)
    return x_pos, y_pos, z_pos

def simmulate(n=20, r=1, dt=1, m=1, vol= 50): #A: n=cantidad de particulas; r= cantidad de frames; dt= dela tiempo en segundos; m= masa de las particulas; vol= volumen en cm3
    K= 1E-1 #1.38064852E-23
    
    pos_x, pos_y, pos_z= random_start(n, vol, vol, vol)
    vx= [0] * n #A: Una lista de n ceros #XXX: Empezar con velocidad
    vy= [0] * n
    vz= [0] * n
    #print('INICIAL \n', pos_x, '\n', pos_y)
    
    salida= open("salida.xyz", "w")

    for l in range(r):
        print(n, file=salida)
        print(" ", file=salida)
        for jj in range(n):
            print("6", pos_x[jj], pos_y[jj], pos_z[jj], "0", file=salida)
        
        paso_x= [] #A: Para guardar las posiciones y despues reemplazar las pos
        paso_y= []
        paso_z= []
        paso_vx= []
        paso_vy= []
        paso_vz= []
        for i in range(n):
            esta_Fx= 0
            esta_Fy= 0
            esta_Fz= 0
            for j in range(n):
                if i != j:
                    #print("Comparando", i, j)
                    esta_D= (pos_x[i]-pos_x[j])**2 + (pos_y[i]-pos_y[j])**2 + (pos_z[i]-pos_z[j])**2 #A: Sacada de la consigna
                    esta_Fx+= (pos_x[i]-pos_x[j])/(esta_D**3) #A: Sacada de la consigna
                    esta_Fy+= (pos_y[i]-pos_y[j])/(esta_D**3)
                    esta_Fz+= (pos_z[i]-pos_z[j])/(esta_D**3)
            esta_Fx= 4*K*esta_Fx #A: Terminando la formula
            esta_Fy= 4*K*esta_Fy
            esta_Fz= 4*K*esta_Fz
            
            esta_vx= vx[i]+(esta_Fx/m)*dt #A: Sacado de la consigna
            esta_vy= vy[i]+(esta_Fy/m)*dt
            esta_vz= vz[i]+(esta_Fz/m)*dt
            
            esta_x= pos_x[i]+esta_vx*dt
            esta_y= pos_y[i]+esta_vy*dt
            esta_z= pos_z[i]+esta_vz*dt
            
            if esta_x > x_size: #A: Si se pasa de largo
                print("ERR", esta_x)
                esta_vx= -esta_vx
                esta_x-= 2*(esta_x-x_size)
            if esta_x < 0:
                esta_vx= -esta_vx
                esta_x-= 2*esta_x
                
            if esta_y > y_size:
                esta_vy= -esta_vy
                esta_y-= 2*(esta_y-y_size)
            if esta_y < 0:
                esta_vy= -esta_vy
                esta_y= -2*esta_y
            
            if esta_z > z_size:
                esta_vz= -esta_vz
                esta_z-= 2*(esta_z-z_size)
            if esta_z < 0:
                esta_vz= -esta_vz
                esta_z-= 2*esta_z
                
            paso_x.append(esta_x)
            paso_y.append(esta_y)
            paso_z.append(esta_z)
            
            paso_vx.append(esta_vx)
            paso_vy.append(esta_vy)
            paso_vz.append(esta_vz)
        #XXX: Guardar las posiciones nuevas
        #print('PASO Nº', l, '\n', paso_x, '\n', paso_y)
        pos_x= paso_x.copy() #A: Actualizo las posiciones
        pos_y= paso_y.copy()
        pos_z= paso_z.copy()
        vx= paso_vx.copy()
        vy= paso_vy.copy()
        vz= paso_vz.copy()
        
    print("END")
simmulate(250, 1000)
#simmulate(30, 100, x_size=100, y_size=100)