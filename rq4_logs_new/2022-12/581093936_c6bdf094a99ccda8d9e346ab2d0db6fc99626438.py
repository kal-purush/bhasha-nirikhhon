
"""
=============================================================================
Filename      : PlotMap
Author        : Sunderam Palavesam
Date Created  : 17 Dec 2022
=============================================================================
Description   : Plot the cities based on lat and lon from input csv
                Developed as part of course work AC
                Developed as part of Python assignment Msc course Work
                Dundee University
credits       : https://towardsdatascience.com/easy-steps-to-plot-geographic-data-on-a-map-python-11217859a2db
Revisions
=============================================================================
17122022   2474761 AC50002  initial version                               1.0
=============================================================================
"""
# pre-requisite - 
#pip install numpy, pandas, matplotlib

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('C:\Sunderam\Masters\ProgLang\pythonassignment2\GrowLocations.csv')

df.head()

print(df)

BBox = (-10.592,   1.6848,  50.681, 57.985)

ruh_m = plt.imread('C:\Sunderam\Masters\ProgLang\pythonassignment2\map7.png')

fig, ax = plt.subplots(figsize = (8,7))
ax.scatter(df.Longitude, df.Latitude, c='b')
ax.set_title('Plotting Grow Data ')
ax.set_xlim(BBox[0],BBox[1])
ax.set_ylim(BBox[2],BBox[3])
ax.imshow(ruh_m, zorder=0, extent = BBox, aspect= 'equal')
plt.show()

