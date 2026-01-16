"""
path.py

Tom Kamstra, Izhar Hamer, Julia Linde

Finds the optimal paths between the chips
"""
from mpl_toolkits import mplot3d
import numpy as np
import matplotlib.pyplot as plt
from code.classes import classes as classs
from code.functions import delete as delete
from code.functions import change_coordinates as change
import copy

import csv

# Create netlist by loading file in class
netlist = classs.Netlist("data/netlist_1.csv").netlist

# Create list for gate coordinates
gate_coordinates = classs.Gate_coordinate("data/pritn_1.csv").gate_coordinates
print(gate_coordinates)

# Create dictionary for gate connections with corresponding shortest distance
distances = {}

for item in netlist:
    gate_start = int(item.gate_1)
    gate_end = int(item.gate_2)
    
    # Create tuple for gates that have to be connected
    connected_gate = (gate_start, gate_end)
    
    # Define coordinates of start and end gate
    coordinate_start = gate_coordinates[gate_start - 1]
    coordinate_end = gate_coordinates[gate_end - 1]
    
    # Define x and y coordinates for start and end gate
    x_coordinate_start = int(coordinate_start[0])
    y_coordinate_start = int(coordinate_start[1])

    x_coordinate_end = int(coordinate_end[0])
    y_coordinate_end = int(coordinate_end[1])

    # Calculate total shortest distance between gates
    total_dist = abs(x_coordinate_start - x_coordinate_end) + abs(y_coordinate_start - y_coordinate_end)

    distances.update({connected_gate: total_dist})

# Sort connections from smallest to largest distance in dictionary
distances = list(distances.items())
for max_number in range(len(distances)-1, -1, -1):
    swapped = False
    for count in range(max_number):
        if distances[count][1] > distances[count + 1][1]:
            distances[count], distances[count + 1] = distances[count + 1], distances[count]
            swapped = True
    if not swapped:
        break

# Create dictionary of wires with connected gates
gate_connections = {}

count = 0

# Saves all wires
allwires = []

# Defines maximum number of layers
max_num_layers = 7

# Connect gates with eachother, starting with smallest distance
for chips in distances:
    gate_start = int(chips[0][0])
    gate_end = int(chips[0][1])
    
    connected_gate = (gate_start, gate_end)

    coordinate_begin = gate_coordinates[gate_start - 1]
    coordinate_end = gate_coordinates[gate_end - 1]
    
    print("COORDINATES")
    print(coordinate_begin)
    print(coordinate_end)
    
    # Define x, y and z coordinates of start and end gate
    x_coordinate_start = int(coordinate_begin[0])
    y_coordinate_start = int(coordinate_begin[1])
    z_coordinate_start = int(coordinate_begin[2])

    x_coordinate_end = int(coordinate_end[0])
    y_coordinate_end = int(coordinate_end[1])
    z_coordinate_end = int(coordinate_end[2])

    # Create list for wire coordinates
    wires = []

    # Define all 5 coordinates that surround current start coordinate
    x_coordinate_startcheck = x_coordinate_start + 1
    coordinate_1 = [x_coordinate_startcheck, y_coordinate_start, z_coordinate_start]
    x_coordinate_startcheck2 = x_coordinate_start - 1
    coordinate_2 = [x_coordinate_startcheck2, y_coordinate_start, z_coordinate_start]
    y_coordinate_startcheck = y_coordinate_start + 1
    coordinate_3 = [x_coordinate_start, y_coordinate_startcheck, z_coordinate_start]
    y_coordinate_startcheck2 = y_coordinate_start - 1
    coordinate_4 = [x_coordinate_start, y_coordinate_startcheck2, z_coordinate_start]
    z_coordinate_startcheck = z_coordinate_start + 1
    coordinate_5 = [x_coordinate_start, y_coordinate_start, z_coordinate_startcheck]
    
    # Saves all coordinates around current start coordinate in list
    coordinate_check = [coordinate_1, coordinate_2, coordinate_3, coordinate_4, coordinate_5]
    
    # Creates list for all coordinates that are already occupied
    all_coordinates = []
    for coo in allwires:
        all_coordinates.append(coo.get_coordinate())

    # Checks whether wire can move in any direction, if at least one coordinate around current coordinate is free
    if all(elem in all_coordinates for elem in coordinate_check):
        for coor in coordinate_check:
            for item_start in allwires:
                if item_start.coordinate == coor and item_start.net[0] != gate_start and item_start.net[1] != gate_start:
                    (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item_start.net, distances, gate_connections, allwires)
                    break
    print("COUNT")
    print(count)
    
    # Create switch variable to switch start moving direction
    if count > len(netlist)+5:
        # Reconnect deleted wires in switched direction
        switch_variable = 1
    else:
        switch_variable = 0
    
    # Overwrite coordinate but save coordinate_begin in different variable        
    coordinate = coordinate_begin
    
    checker = 0
    notcomplete = 0
    
    while coordinate != coordinate_end:
        # Determine direction in which wire has to move
        if x_coordinate_start > x_coordinate_end:
            step_x = -1
        elif x_coordinate_start < x_coordinate_end:
            step_x = 1

        if y_coordinate_start > y_coordinate_end:
            step_y = -1
        elif y_coordinate_start < y_coordinate_end:
            step_y = 1
 
        # Append start coordinate to wire
        wires.append(coordinate)
        print(coordinate)
        wire = classs.Wire(coordinate, connected_gate)
        allwires.append(wire)
        
        if switch_variable == 0:
            # Loop until x-coordinate from start gate equals x-coordinate from end gate
            while x_coordinate_start != x_coordinate_end:
                x_coordinate_start = x_coordinate_start + step_x
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                # Check for other gates or other wires
                if gate_connections:
                    (x_coordinate_start, z_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, x_coordinate_start, -step_x, z_coordinate_start, 1)
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    (z_coordinate_start, y_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, z_coordinate_start, -1, y_coordinate_start, step_y)
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    # Coordinate changes 2 times in y-direction, so step_y is doubled
                    (y_coor_notrelevant, y_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, y_coordinate_start, step_y, y_coordinate_start, (-2*step_y))
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    (y_coordinate_start, x_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, y_coordinate_start, step_y, x_coordinate_start, -step_x)
                elif coordinate in gate_coordinates and coordinate != coordinate_end:
                    x_coordinate_start = x_coordinate_start - step_x
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    z_coordinate_start = z_coordinate_start + 1
                    #checken of na deze stap geen gate zit
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    (z_coordinate_start, x_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, z_coordinate_start, -1, x_coordinate_start, -step_x)
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    (x_coordinate_start, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, step_x, y_coordinate_start, step_y)
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    # Coordinate changes 2 times in y-direction, so step_y is doubled
                    (y_coor_notrelevant, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, y_coordinate_start, step_y, y_coordinate_start, (-2*step_y))
        
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                wires.append(coordinate)
                print(coordinate)
                wire = classs.Wire(coordinate, connected_gate)
                allwires.append(wire)
            
            # Redefine step in direction of y-coordinate
            if y_coordinate_start < y_coordinate_end:
                step_y = 1
            elif y_coordinate_start > y_coordinate_end:
                step_y = -1
            
            # Change z-coordinate if x- and y-coordinates are same as those from end gate        
            if x_coordinate_start == x_coordinate_end and y_coordinate_start == y_coordinate_end:
                while z_coordinate_start != z_coordinate_end:
                    z_coordinate_start = z_coordinate_start - 1
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    if gate_connections:
                        (z_coordinate_start, y_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, z_coordinate_start, 1, y_coordinate_start, step_y)
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        (y_coordinate_start, x_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, y_coordinate_start, -step_y, x_coordinate_start, step_x)
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        (x_coor_notrelevant, x_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, x_coordinate_start, step_x, x_coordinate_start, (-2*step_x))
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        (x_coordinate_start, y_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, x_coordinate_start, step_x, y_coordinate_start, -step_y)
                    elif coordinate in gate_coordinates and coordinate != coordinate_end:
                        z_coordinate_start = z_coordinate_start + 1
                        y_coordinate_start = y_coordinate_start + step_y
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        (x_coordinate_start, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, step_x, y_coordinate_start, -step_y)
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        (x_coor_notrelevant, x_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, step_x, x_coordinate_start, (-2*step_x))
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        (x_coordinate_start, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, step_x, y_coordinate_start, -step_y)
         
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    wires.append(coordinate)
                    print(coordinate)
                    wire = classs.Wire(coordinate, connected_gate)
                    allwires.append(wire)
                    
        # Redefine step in direction of y-coordinate    
        if y_coordinate_start < y_coordinate_end:
            step_y = 1
        elif y_coordinate_start > y_coordinate_end:
            step_y = -1
        
        # Loop until y-coordinate from start gate equals y-coordinate from end gate
        while y_coordinate_start != y_coordinate_end:
           y_coordinate_start = y_coordinate_start + step_y
           coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
           # Check for other gates or other wires
           if gate_connections:
               try:
                   step_x = step_x
               except:
                   step_x = 0
               (y_coordinate_start, z_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, y_coordinate_start, -step_y, z_coordinate_start, 1)
               coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
               (z_coordinate_start, x_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, z_coordinate_start, -1, x_coordinate_start, step_x)
               coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
               (x_coor_notrelevant, x_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, x_coordinate_start, step_x, x_coordinate_start, (-2*step_x))
               coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
               (x_coordinate_start, y_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, x_coordinate_start, step_x, y_coordinate_start, -step_y)
           elif coordinate in gate_coordinates and coordinate != coordinate_end:
               y_coordinate_start = y_coordinate_start - step_y
               z_coordinate_start = z_coordinate_start + 1
               coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
               (x_coordinate_start, z_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, step_x, z_coordinate_start, -1)
               coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
               (x_coor_notrelevant, x_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, step_x, x_coordinate_start, (-2*step_x))
               coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
               (x_coordinate_start, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, step_x, y_coordinate_start, -step_y)

           # Reset switch variable to be able to move in x-direction
           switch_variable = 0
           
           coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
           wires.append(coordinate)
           print(coordinate)
           wire = classs.Wire(coordinate, connected_gate)
           allwires.append(wire)
           
           # Redefine step in direction of y-coordinate
           if y_coordinate_start < y_coordinate_end:
               step_y = 1
           elif y_coordinate_start > y_coordinate_end:
               step_y = -1
           
           # Change z-coordinate if x- and y-coordinates are same as those from end gate
           if x_coordinate_start == x_coordinate_end and y_coordinate_start == y_coordinate_end:
               while z_coordinate_start != z_coordinate_end:
                   z_coordinate_start = z_coordinate_start - 1
                   coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                   if gate_connections:
                       (z_coordinate_start, x_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, z_coordinate_start, 1, x_coordinate_start, step_x)
                       coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                       (x_coordinate_start, y_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, x_coordinate_start, -step_x, y_coordinate_start, step_y)
                       coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                       (y_coor_notrelevant, y_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, y_coordinate_start, step_y, y_coordinate_start, (-2*step_y))
                       coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                       (y_coordinate_start, x_coordinate_start) = change.change_coor(gate_connections, coordinate, gate_coordinates, wires, coordinate_end, y_coordinate_start, step_y, x_coordinate_start, -step_x)
                   elif coordinate in gate_coordinates and coordinate != coordinate_end:
                       z_coordinate_start = z_coordinate_start + 1
                       x_coordinate_start = x_coordinate_start + step_x
                       coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                       (x_coordinate_start, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, -step_x, y_coordinate_start, step_y)
                       coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                       (y_coor_notrelevant, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, y_coordinate_start, step_y, y_coordinate_start, (-2*step_y))
                       coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                       (x_coordinate_start, y_coordinate_start) = change.change_coor2(coordinate, wires, gate_coordinates, coordinate_end, x_coordinate_start, -step_x, y_coordinate_start, step_y)
                                   
                   coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                   wires.append(coordinate)
                   print(coordinate)
                   wire = classs.Wire(coordinate, connected_gate)
                   allwires.append(wire)
                   
        # Check whether wire isn't running into forever loop
        if len(wires) > 100:
            x_coordinate_check = x_coordinate_end + step_x
            check_coordinate = [x_coordinate_check, y_coordinate_end, z_coordinate_end]
            for item in allwires:
                if item.coordinate == check_coordinate and item.net[0] != gate_end and item.net[1] != gate_end:
                    (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                    break
                else:
                    copy_gate_connections = copy.deepcopy(gate_connections)
                    for key in copy_gate_connections:
                        if len(copy_gate_connections[key]) > 75:
                            (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                break            
            x_coordinate_check = x_coordinate_end - step_x
            check_coordinate = [x_coordinate_check, y_coordinate_end, z_coordinate_end]
            for item in allwires:
                if item.coordinate == check_coordinate and item.net[0] != gate_end and item.net[1] != gate_end:
                    (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                    break
                else:
                    copy_gate_connections = copy.deepcopy(gate_connections)
                    for key in copy_gate_connections:
                        if len(copy_gate_connections[key]) > 75:
                            (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                break
            y_coordinate_check = y_coordinate_end + step_y
            check_coordinate = [x_coordinate_end, y_coordinate_check, z_coordinate_end]
            for item in allwires:
                if item.coordinate == check_coordinate and item.net[0] != gate_end and item.net[1] != gate_end:
                    (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                    break
                else:
                    copy_gate_connections = copy.deepcopy(gate_connections)
                    for key in copy_gate_connections:
                        if len(copy_gate_connections[key]) > 75:
                            (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                break
            y_coordinate_check = y_coordinate_end - step_y
            check_coordinate = [x_coordinate_end, y_coordinate_check, z_coordinate_end]
            for item in allwires:
                if item.coordinate == check_coordinate and item.net[0] != gate_end and item.net[1] != gate_end:
                    (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                    break
                else:
                    copy_gate_connections = copy.deepcopy(gate_connections)
                    for key in copy_gate_connections:
                        if len(copy_gate_connections[key]) > 75:
                            (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, key, distances, gate_connections, allwires)
                break
            z_coordinate_check = z_coordinate_end + 1
            check_coordinate = [x_coordinate_end, y_coordinate_end, z_coordinate_check]
            for item in allwires:
                if item.coordinate == check_coordinate and item.net[0] != gate_end and item.net[1] != gate_end:
                    (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, item.net, distances, gate_connections, allwires)
                    break
                else:
                    copy_gate_connections = copy.deepcopy(gate_connections)
                    for key in copy_gate_connections:
                        if len(copy_gate_connections[key]) > 75:
                            (wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires) = delete.delete_wire(wires, coordinate_begin, key, distances, gate_connections, allwires)
                break
                    
            # If no wire can be deleted and current wire can still not reach end gate
            wires = []
            x_coordinate_start = int(coordinate_begin[0])
            y_coordinate_start = int(coordinate_begin[1])
            z_coordinate_start = int(coordinate_begin[2])
            coordinate = coordinate_begin
            # Check y-coordinate first, then x-coordinate
            # Change value of switch variable to start moving in other direction
            switch_variable = 1
        else: 
            checker += 1
            print("CHECKER", checker)
        if checker > 200: 
            notcomplete += 1
            print("NOTCOMPLETE", notcomplete)
            break
               
    count += 1         
    print("WIRESSSS")
    print(wires)
    
    wires_length = len(wires)
    
    # Delete part of wire when wire goes back and forth on one line
    if wires_length > 4:
        indices = []
        for count in range(wires_length-2):
            coor_1 = wires[count]
            coor_2 = wires[count + 1]
            coor_3 = wires[count + 2]

            # Save indices of wires list
            if coor_1 == coor_3:
                if count not in indices and (count + 1) not in indices:
                    indices.append(count)
                    indices.append(count + 1)
        
        # Delete wire coordinate with highest index first
        for delete_count in range(wires_length):
            for index in indices:
                if (wires_length - delete_count) == index:
                    wires.pop(index)
    
    net = classs.Net(gate_start, gate_end)
    net.create_wires(wires)
    gate_connections.update({connected_gate: wires})
    # if count > 54:
   #      break

    # if len(gate_connections) == len(netlist):
   #      break
    print("ALL WIRES")
    print(allwires)
    print(net)
    
print(gate_connections)
print(len(gate_connections))
print("JOEJOE")
print("ALL WIRESSS")
print(allwires[0].coordinate)
# print(gate_connections[(17,10)])


length = 0
# Calculate total length of wires
for key in gate_connections:
    wire = gate_connections[key]
    length = length + len(wire)
    
print("TOTAL LENGTH")
print(length)


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
# plt.pause(3)
for gate_coordinate in gate_coordinates: 
    set_gate(gate_coordinate)
    plt.pause(0.03)

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
            print("LineFromTo", allconnectionlist[i], "To",allconnectionlist[i + 1] )
            draw_line(allconnectionlist[i], allconnectionlist[i+1], colours[colourcounter] )
            plt.pause(0.000001)
        except: 
            break
            
with open('output.csv', mode= 'w') as outputfile:
    output_writer = csv.writer(outputfile, delimiter= ',')

    for keys in gate_connections:
        output_writer.writerow([keys, gate_connections[keys]])

ax.set_xlabel('x')
ax.set_ylabel('y')
ax.set_zlabel('z')

plt.show()