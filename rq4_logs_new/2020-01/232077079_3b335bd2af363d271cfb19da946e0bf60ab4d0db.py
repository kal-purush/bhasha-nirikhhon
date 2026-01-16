"""
path.py

Tom Kamstra, Izhar Hamer, Julia Linde

Finds the optimal paths between the chips
"""

import csv
# from pyplot import draw_line, set_gate, make_grid
from mpl_toolkits import mplot3d
import numpy as np
import matplotlib.pyplot as plt

data = open("netlist_1.csv")
reader = csv.reader(data)

# Create netlist
netlist = []

for net_1, net_2 in reader:
    listje = (net_1, net_2)
    netlist.append(listje)

gates = open("pritn_1.csv")
reader = csv.reader(gates)

# Create list for gate coordinates
gate_coordinates = []

for number, x, y in reader:
    if x != " x":
        x = int(x)
        y = int(y)
        listje = [x, y, 0]
        gate_coordinates.append(listje)

print("gateeee coordinates")
print(gate_coordinates)
# Create wires with connections
gate_connections = {}

# Create gate connections with corresponding shortest distance
distances = {}

for chip1, chip2 in netlist:
    if chip1 != "chip_a":
        gate_1 = int(chip1)
        gate_2 = int(chip2)
    
        connected_gate = (gate_1, gate_2)
    
        coordinate_start = gate_coordinates[gate_1 - 1]
        coordinate_end = gate_coordinates[gate_2 - 1]
    
        x_coordinate_1 = int(coordinate_start[0])
        y_coordinate_1 = int(coordinate_start[1])

        x_coordinate_2 = int(coordinate_end[0])
        y_coordinate_2 = int(coordinate_end[1])
    
        total_dist = abs(x_coordinate_1 - x_coordinate_2) + abs(y_coordinate_1 - y_coordinate_2)
    
        distances.update({connected_gate: total_dist})

print("JOEJOE")
print(distances)


# for key, value in distances.items():
#     if distances[key] > distances[key + 1]:
#         temporary = distances[key]
#         distances[key] = distances[key + 1]
#         distances[key + 1] = temporary

distances = list(distances.items())
for mx in range(len(distances)-1, -1, -1):
    swapped = False
    for i in range(mx):
        if distances[i][1] > distances[i+1][1]:
            distances[i], distances[i+1] = distances[i+1], distances[i]
            swapped = True
    if not swapped:
        break

print("JOEJOEEEE")
print(distances)

for chip1, chip2 in netlist:
    print("CHIP 1!!!!\n")
    print(chip1)
    if chip1 != "chip_a":
        gate_1 = int(chip1)
        gate_2 = int(chip2)
        
        connected_gate = (gate_1, gate_2)

        coordinate_start = gate_coordinates[gate_1 -1]
        coordinate_end = gate_coordinates[gate_2- 1]       
        print("BEGIN COORDINATE")
        print(coordinate_start)
        print("END COORDINATE")
        print(coordinate_end)

        x_coordinate_1 = int(coordinate_start[0])
        y_coordinate_1 = int(coordinate_start[1])
        z_coordinate_1 = int(coordinate_start[2])

        x_coordinate_2 = int(coordinate_end[0])
        y_coordinate_2 = int(coordinate_end[1])
        z_coordinate_2 = int(coordinate_end[2])

        wires = []

        while coordinate_start != coordinate_end:
            if x_coordinate_1 < x_coordinate_2:
                step_x = 1
            elif x_coordinate_1 > x_coordinate_2:
                step_x = -1
        
            if y_coordinate_1 < y_coordinate_2:
                step_y = 1
            elif y_coordinate_1 > y_coordinate_2:
                step_y = -1

            wires.append(coordinate_start)
        
            while x_coordinate_1 != x_coordinate_2:
                x_coordinate_1 = x_coordinate_1 + step_x
                coordinate_start = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                # Check for other gates or other wires
                if gate_connections:
                    for key in gate_connections:
                        selected_wires = gate_connections[key]
                        if coordinate_start in selected_wires or coordinate_start in gate_coordinates:
                            if coordinate_start != coordinate_end:
                                x_coordinate_1 = x_coordinate_1 - step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                z_coordinate_1 = z_coordinate_1 + 1
                                #checken of na deze stap geen gate zit
                                break
                elif coordinate_start in gate_coordinates and coordinate_start != coordinate_end:
                    x_coordinate_1 = x_coordinate_1 - step_x
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    z_coordinate_1 = z_coordinate_1 + 1
                    #checken of na deze stap geen gate zit
        
                
                coordinate_start = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                # if coordinate gelijk aan andere: doe - step_x en y_coordinate + step_y
                wires.append(coordinate_start)
    
                if x_coordinate_1 == x_coordinate_2 and y_coordinate_1 == y_coordinate_2:
                    while z_coordinate_1 != z_coordinate_2:
                        z_coordinate_1 = z_coordinate_1 - 1
                        coordinate_start = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                        wires.append(coordinate_start)
    

            while y_coordinate_1 != y_coordinate_2:
                y_coordinate_1 = y_coordinate_1 + step_y
                coordinate_start = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                # Check for other gates or other wires
                if gate_connections:
                    for key in gate_connections:
                        selected_wires = gate_connections[key]
                        if coordinate_start in selected_wires or coordinate_start in gate_coordinates:
                            if coordinate_start != coordinate_end:
                                y_coordinate_1 = y_coordinate_1 - step_y
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                z_coordinate_1 = z_coordinate_1 + 1
                                #checken of na deze stap geen gate zit
                                break
                elif coordinate_start in gate_coordinates and coordinate_start != coordinate_end:
                    y_coordinate_1 = y_coordinate_1 - step_y
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    z_coordinate_1 = z_coordinate_1 + 1
                    #checken of na deze stap geen gate zit
    
                coordinate_start = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                # if coordinate gelijk aan andere: doe - step_y en x_coordinate + step_x
                wires.append(coordinate_start)
        
                if x_coordinate_1 == x_coordinate_2 and y_coordinate_1 == y_coordinate_2:
                    while z_coordinate_1 != z_coordinate_2:
                        z_coordinate_1 = z_coordinate_1 - 1
                        coordinate_start = [x_coordinate_1, y_coordinate_1, z_coordinate_1]
                        wires.append(coordinate_start)

    
        print("NEW WIRES")
        print(wires)
        gate_connections.update({connected_gate: wires})

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
            print("LineFromTo", allconnectionlist[i],  allconnectionlist[i + 1])
            draw_line(allconnectionlist[i], allconnectionlist[i+1], colours[colourcounter] )
        except: 
            break



ax.set_xlabel('x')
ax.set_ylabel('y')
ax.set_zlabel('z')

plt.show()