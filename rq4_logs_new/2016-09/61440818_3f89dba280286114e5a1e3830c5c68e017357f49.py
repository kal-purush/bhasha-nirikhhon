# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 11:32:15 2016

@author: nathan
"""
import numpy as np
import matplotlib.pyplot as plt

with open("mylog.log") as f:
    data = f.readlines()
    data = data[20:]

x = [row.split()[4] for row in data]
y1 = [(float(row.split()[5])) for row in data]
y2 = [(float(row.split()[6])) for row in data]
y3 = [(float(row.split()[7])) for row in data]
y4 = [(float(row.split()[8])) for row in data]
y5 = [(float(row.split()[2])) for row in data]
fig1 = plt.figure()
x2=[(float(row.split()[4])**(1.0/3.0))*.396/(2*np.sqrt(2)) for row in data]
ax1 = fig1.add_subplot(111)

var1='Pressure'

ax1.set_title(var1)    
ax1.set_xlabel('Nearest Neighbor distance (nm)')
ax1.set_ylabel(var1)
#ax3.set_title(var3)    
#ax3.set_xlabel('Timestep')
#ax3.set_ylabel(var3)
ax1.plot(x2,y1, c='r', label='Pressure')

leg1 = ax1.legend()

fig2 = plt.figure()

ax2 = fig2.add_subplot(111)

var2='Energy'

ax2.set_title(var2)    
ax2.set_xlabel('Nearest Neighbor distance (nm)')
ax2.set_ylabel(var2)
#ax3.set_title(var3)    
#ax3.set_xlabel('Timestep')
#ax3.set_ylabel(var3)

ax2.plot(x2,y2, c='g', label='LJ Energy')
ax2.plot(x2,y3, c='b', label='Bond Energy')
ax2.plot(x2,y4, c='black', label='Angle Energy')
ax2.plot(x2,y5, c='orange', label='Total Potential Energy')
leg2 = ax2.legend()
plt.show()