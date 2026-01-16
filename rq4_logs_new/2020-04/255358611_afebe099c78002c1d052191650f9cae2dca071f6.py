from math import cos, sin, atan2, sqrt
import matplotlib.pyplot as plt
from mat
plt.clf()
width, height = plt.rcParams['figure.figsize']
size = min(width, height)
# make a square figure
fig = plt.figure(figsize=(size, size))
ax = fig.add_axes([0.1, 0.1, 0.8, 0.8], polar=True) #axisbg='#d5de9c'
x = range(10)
y = range(10)
r = []
phi = []
for ii in range(len(x)):
  r.append(sqrt(x[ii]**2.+y[ii]**2.))
  phi.append(atan2(y[ii],x[ii]))
ax.scatter(phi,r)
plt.show()