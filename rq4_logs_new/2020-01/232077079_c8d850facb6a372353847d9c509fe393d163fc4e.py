"""
path.py
Tom Kamstra, Izhar Hamer, Julia Linde
Finds the optimal paths between the chips
"""
from mpl_toolkits import mplot3d
import numpy as np
import matplotlib.pyplot as plt
import random

import csv

# Open file with netlist
data = open("netlist_1.csv")
reader = csv.reader(data)

# Create netlist
netlist = []

for net_1, net_2 in reader:
    net = (net_1, net_2)
    netlist.append(net)

# Open file with gates
gates = open("pritn_1.csv")
reader = csv.reader(gates)

# Create list for gate coordinates
gate_coordinates = []

for number, x, y in reader:
    if x != " x":
        x = int(x)
        y = int(y)
        coordinates = [x, y, 0]
        gate_coordinates.append(coordinates)

random.shuffle(gate_coordinates)

# Create dictionary of wires with connected gates
gate_connections = {}

# Connect gates with eachother, starting with smallest distance
for chips in gate_coordinates:
    gate_1 = int(chips[0])
    gate_2 = int(chips[1])
    
    connected_gate = (gate_1, gate_2)

    coordinate = gate_coordinates[gate_1 - 1]
    coordinate_end = gate_coordinates[gate_2 - 1]

    x_coordinate_1 = int(coordinate[0])
    y_coordinate_1 = int(coordinate[1])
    z_coordinate_1 = int(coordinate[2])

    x_coordinate_2 = int(coordinate_end[0])
    y_coordinate_2 = int(coordinate_end[1])
    z_coordinate_2 = int(coordinate_end[2])

    wires = []

    while coordinate != coordinate_end:
        if x_coordinate_1 < x_coordinate_2:
            step_x = 1
        elif x_coordinate_1 > x_coordinate_2:
            step_x = -1
    
        if y_coordinate_1 < y_coordinate_2:
            step_y = 1
        elif y_coordinate_1 > y_coordinate_2:
            step_y = -1

        wires.append(coordinate)
    
        while x_coordinate_1 != x_coordinate_2:
            x_coordinate_1 = x_coordinate_1 + step_x
            coordinate = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
            # Check for other gates or other wires
            if gate_connections:
                for key in gate_connections:
                    selected_wires = gate_connections[key]
                    if coordinate in selected_wires or coordinate in gate_coordinates:
                        if coordinate != coordinate_end:
                            x_coordinate_1 = x_coordinate_1 - step_x
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            z_coordinate_1 = z_coordinate_1 + 1
                            #checken of na deze stap geen gate zit
                            break
            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                x_coordinate_1 = x_coordinate_1 - step_x
                # z kan nu niet meerdere stappen omhoog/omlaag
                z_coordinate_1 = z_coordinate_1 + 1
                #checken of na deze stap geen gate zit
    
            coordinate = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
            wires.append(coordinate)

            if x_coordinate_1 == x_coordinate_2 and y_coordinate_1 == y_coordinate_2:
                while z_coordinate_1 != z_coordinate_2:
                    z_coordinate_1 = z_coordinate_1 - 1
                    coordinate = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                    wires.append(coordinate)

        while y_coordinate_1 != y_coordinate_2:
            y_coordinate_1 = y_coordinate_1 + step_y
            coordinate = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
            # Check for other gates or other wires
            if gate_connections:
                for key in gate_connections:
                    selected_wires = gate_connections[key]
                    if coordinate in selected_wires or coordinate in gate_coordinates:
                        if coordinate != coordinate_end:
                            y_coordinate_1 = y_coordinate_1 - step_y
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            z_coordinate_1 = z_coordinate_1 + 1
                            #checken of na deze stap geen gate zit
                            break
            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                y_coordinate_1 = y_coordinate_1 - step_y
                # z kan nu niet meerdere stappen omhoog/omlaag
                z_coordinate_1 = z_coordinate_1 + 1
                #checken of na deze stap geen gate zit

            coordinate = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
            wires.append(coordinate)
    
            if x_coordinate_1 == x_coordinate_2 and y_coordinate_1 == y_coordinate_2:
                while z_coordinate_1 != z_coordinate_2:
                    z_coordinate_1 = z_coordinate_1 - 1
                    coordinate = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                    if gate_connections:
                        for key in gate_connections:
                            selected_wires = gate_connections[key]
                            if coordinate in selected_wires or coordinate in gate_coordinates:
                                if coordinate != coordinate_end:
                                    z_coordinate_1 = z_coordinate_1 + 1
                                    # z kan nu niet meerdere stappen omhoog/omlaag
                                    x_coordinate_1 = x_coordinate_1 + step_x
                                    #checken of na deze stap geen gate zit
                                    break
                                    # moet ook uit while loop breken!
                    coordinate = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                    wires.append(coordinate)
                    
    gate_connections.update({connected_gate: wires})
print(gate_connections)
print("JOEJOE")
# print(gate_connections[(17,10)])


def make_grid(layers, size):
    for i in range(layers): 
        GridX = np.linspace(0, size, (size + 1))
        GridY = np.linspace(0, size, (size + 1))
        X, Y = np.meshgrid(GridX, GridY)
        Z = (np.sin(np.sqrt(X ** 2 + Y ** 2)) * 0) + i
        # Plot grid
        # ax.plot_wireframe(X, Y, Z, lw=0.5,  color='grey')
    #configure axes
    ax.set_zlim3d(0, layers)
    ax.set_xlim3d(0, size)
    ax.set_ylim3d(0, size)

# Enter coordinates as list with: [X, Y, Z]
def draw_line(crdFrom, crdTo, colour):  
    Xline = [crdFrom[0], crdTo[0]]
    Yline = [crdFrom[1], crdTo[1]]
    Zline = [crdFrom[2], crdTo[2]]
    # Draw line
    ax.plot(Xline, Yline, Zline,lw=2,  color=colour, ms=12)

def set_gate(crd):
    PointX = [crd[0]]
    PointY = [crd[1]]
    PointZ = [crd[2]]
    # Plot points
    ax.plot(PointX, PointY, PointZ, ls="None", marker="o", color='red')


fig = plt.figure()
ax = plt.axes(projection="3d")

make_grid(8, 16)

for gate_coordinate in gate_coordinates: 
    set_gate(gate_coordinate)

allConnections = []
colours = ['b','lightgreen','cyan','m','yellow','k', 'pink']
colourcounter = 0
for keys in gate_connections:
    allConnections = gate_connections[keys]
    allconnectionlist = []
    for listconnection in allConnections: 
        allconnectionlist.append(listconnection)
    if colourcounter < 6:
        colourcounter += 1
    else: 
        colourcounter = 0
    for i in range(len(allconnectionlist)):
        try:
            print("LineFromTo", allconnectionlist[i],  allconnectionlist[i + 1], colours[colourcounter]  )
            draw_line(allconnectionlist[i], allconnectionlist[i+1], colours[colourcounter] )
        except: 
            break



ax.set_xlabel('x')
ax.set_ylabel('y')
ax.set_zlabel('z')

plt.show()