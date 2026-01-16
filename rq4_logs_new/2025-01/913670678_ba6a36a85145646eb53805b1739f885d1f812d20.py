import numpy as np
import matplotlib.pyplot as plt
from random import random 
import time


height = np.sqrt(3)/2

x_vertex = np.array([-0.5, 0.5, 0.0])
y_vertex = np.array([0.0, 0.0, height])

fig, ax = plt.subplots(dpi=300)
ax.set_aspect('equal', adjustable='box')
ax.plot([x_vertex[0], x_vertex[1], x_vertex[2], x_vertex[0]],
         [y_vertex[0], y_vertex[1], y_vertex[2], y_vertex[0]], lw=0.5)





iterations = 500_000


def create_point():
    point = np.random.rand(2)
    point[0] -= 0.5
    point[1] *= height
    return point

def create_vertex():
    vertex_int = np.random.randint(0,3)
    return np.array([x_vertex[vertex_int], y_vertex[vertex_int]])

def find_midpoint(point, vertex):
    return np.array([(point[0]+vertex[0])/2, (point[1]+vertex[1])/2])

#Create the starting point
point = create_point()

#Create empty array to fill with the fractal points
points = np.zeros((iterations, 2))

#Start the timer
start_time = time.time()

for i in range(iterations):

    if point[1] > -height/0.5*np.abs(point[0]) + height: #outside
        point[1] = height-point[1]
        point[0] = np.sign(point[0]) * (0.5-np.abs(point[0]))

    vertex = create_vertex()
    midpoint = find_midpoint(point, create_vertex())
    
    points[i] = point
    point = midpoint

#Finish the timer
print("--- %s seconds ---" % (time.time() - start_time))


with open('serpinsky_points.tsv','ab') as f:
    np.savetxt(f,points,delimiter="\t")

#np.savetxt("serpinsky_points.tsv", points, delimiter="\t")



ax.scatter(points[:,0], points[:,1], marker='.', s=0.15, linewidth=0,  color='tab:orange')
plt.show()

