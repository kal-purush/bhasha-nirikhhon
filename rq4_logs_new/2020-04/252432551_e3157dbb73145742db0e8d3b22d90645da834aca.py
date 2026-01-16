# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import matplotlib
matplotlib.use("Qt5Agg")

import matplotlib.pyplot as plt
from math import sin,pi
from matplotlib.animation import FuncAnimation

from symulacja import Populacja

pop = Populacja(30)


fig, ax = plt.subplots()

wykresy = {'zdrowy'  : plt.plot([],[], 'go')[0],
           'chory'   : plt.plot([],[], 'ro')[0],
           'nosiciel': plt.plot([],[], 'yo')[0] }

def init():
    ax.set_xlim(0, pop.szerokosc)
    ax.set_ylim(0, pop.wysokosc)
    return wykresy.values()

def update(frame):
    pop.ruch()
    
    for status,wykres in wykresy.items():
        xdata = [p.x for p in pop._pacjenci if p.status == status]
        ydata = [p.y for p in pop._pacjenci if p.status == status]
        wykres.set_data(xdata, ydata)
        
    return wykresy.values()

ani = FuncAnimation(fig, update, frames=None,
                    init_func=init, blit=True, interval=50)
plt.show(block=True)