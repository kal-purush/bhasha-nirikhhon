import numpy as np
from numpy.linalg import *
import matplotlib.pyplot as plt
from scipy.linalg import lu_factor
from scipy.linalg import lu_solve
import math as math
import sys
import scipy.linalg as sp
import time




def x_axe_mono(epaisseur, dx):
    Nx = int(epaisseur / dx)
    x = np.zeros(Nx)
    for i in range(Nx)     :
        x[i] = i*dx
    return x



"==============Solution analytique==================="
def anal_sol(lbda,rho,cp,epaisseur,dx,temps):
    
    # Dt = np.zeros((Nt))
    Nx = int(epaisseur / dx)
    # for i in range(Nt):
    #     Dt[i] = i*dt
    temp = np.zeros((Nx))
    temp[0] = 20
    temp[Nx-1] = 30
    alpha = lbda/(rho*cp)
    
    Dx = x_axe_mono(epaisseur, dx)
    
    T0 = 0
    teta0 = (T0 - temp[0])/(temp[0] - temp[Nx-1]) 
    for i in range (1,len(Dx)-1):
        
        summ=0

        
        for j in range (1,100):
            
            # summ = 1/n*(np.exp(-(n*np.pi/(2*epaisseur)*alpha*temps)))*np.sin(n*np.pi*i*dx/(2*epaisseur))
            
            # n=n+2
            Cn = 2 * ((-1)**j*(1-teta0) + teta0)/( j * np.pi)
            
            summ = summ + Cn * np.sin(j * np.pi * i*dx / epaisseur) * np.exp(-(j*np.pi/epaisseur)**2 * alpha * temps)
            
        temp[i] = temp[0] + i*dx*(temp[Nx-1] - temp[0])/epaisseur + (temp[Nx-1] - temp[0])*summ
    
    
    
    return temp