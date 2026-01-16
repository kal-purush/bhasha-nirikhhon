import numpy as np
from matplotlib import pyplot as plt
import time, sys

nx = 41             # number of distance steps
dx = 2 / (nx - 1)   # amount of distance each step covers (m)
nt = 25             # number of time steps
dt = 0.025          # amount of time each time step covers (s)
c = 1               # wavespeed (m/s)

u = np.ones(nx)
u[int(0.5 / dx):int(1 / dx + 1)] = 2  # sets u = 2 between 0.5 and 1, initial condition
#print(u) 

un = np.ones(nx)    # initialize a temporary array

for n in range(nt):
    un = u.copy()                ## copy the existing values of u into un
    for i in range(1, nx):
        u[i] = un[i] - c * dt / dx * (un[i] - un[i - 1])

plt.plot(np.linspace(0, 2, nx), u)
plt.show()
 