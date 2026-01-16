import matplotlib.pyplot as plt
import numpy as np
import csv
from scipy.spatial import ConvexHull

x = []
y = []

with open('ex.txt','r') as csvfile:
    plots = csv.reader(csvfile, delimiter=',')
    for row in plots:
        x.append(float(row[0]))
        y.append(float(row[1]))

#plt.scatter(x,y,s=10, c = 'r',label='rate pairs')
plt.xlabel('Rate 1')
plt.ylabel('Rate 2')
plt.title('Achievable rates in a two-user DMAC')
#plt.legend()

points= np.loadtxt('ex.txt', unpack= False, delimiter=',')

##code for sum-capacity line
#t = np.linspace(0, 0.204, 1000)
#plt.plot(t, 0.204 - t, linestyle='solid')


##code for convex hull
hull = ConvexHull(points)
#plt.plot(points[:,0], points[:,1], 'o')
for simplex in hull.simplices:
	plt.plot(points[simplex, 0], points[simplex, 1], 'b-')
plt.grid(True)
plt.show()