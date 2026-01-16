from code.visualisation import plot as plot
from code.classes import classes as classs
from code.functions import astardelete as astardelete                     
from code.algorithms import Astar2 as Astar
import copy
import matplotlib.pyplot as plt
import time
import csv



if __name__ == '__main__':
    start_time = time.time()
    # Create netlist by loading file in class
    netlist = classs.Netlist("data/example_net3.csv").netlist

    # Create list for gate coordinates
    gate_coordinates = classs.Gate_coordinate("data/example_prit3.csv").gate_coordinates
  
    gate_connections = {}

    

   
    
    """
    # TODO
        geef de begin en eindgate mee
        alle gate_coordinaten
        geef een lijst mee met coordinaten waar al draad ligt
    """ 
    ax = plot.make_grid(8, 5)
    # string_gates = [] 
    blocked = []
    allwires = []


    for gate_coordinate in gate_coordinates: 
        # blocked.append(Astar.Node(gate_coordinate[0], gate_coordinate[1], gate_coordinate[2]).set_blocked())
        blocked.append(str(gate_coordinate))
        plot.set_gate(gate_coordinate, ax)
            
    distances = {}
    # for net in netlist: 
    #     start = gate_coordinates[int(net.gate_1) - 1]
    #     goal = gate_coordinates[int(net.gate_2) - 1]

    for item in netlist:
        gate_start = int(item.gate_1)
        gate_end = int(item.gate_2)
                    
        # Create tuple for gates that have to be connected
        connected_gate = (gate_start, gate_end)
        
        coordinate_start = gate_coordinates[gate_start - 1]
        coordinate_end = gate_coordinates[gate_end - 1]

        x_coordinate_1 = int(coordinate_start[0])
        y_coordinate_1 = int(coordinate_start[1])

        x_coordinate_2 = int(coordinate_end[0])
        y_coordinate_2 = int(coordinate_end[1])

        # Calculate total shortest distance between gates
        total_dist = abs(x_coordinate_1 - x_coordinate_2) + abs(y_coordinate_1 - y_coordinate_2)

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

    for chips in distances:
        gate_start = int(chips[0][0])
        gate_end = int(chips[0][1])
    
        connected_gate = (gate_start, gate_end)

        coordinate_begin = gate_coordinates[gate_start - 1]
        coordinate_end = gate_coordinates[gate_end - 1]

        grid = Astar.make_grid()

        a_star_path = Astar.a_star(coordinate_begin, coordinate_end, grid)
        end_time_3 = time.time()
        print(a_star_path)
        gate_connections.update({connected_gate: a_star_path})




    print(gate_connections)

    end_time = time.time()
    print("TIME: ", end_time - start_time)

    # allConnections = []
    # colours = ['b','lightgreen','cyan','m','yellow','k', 'pink']
    # colourcounter = 0
    # for keys in gate_connections:
    #     allConnections = gate_connections[keys]
    #     print(len(allConnections))
    #     allconnectionlist = []
    #     for listconnection in allConnections: 
    #         allconnectionlist.append(listconnection)
    #     if colourcounter < 6:
    #         colourcounter += 1
    #     else: 
    #         colourcounter = 0

    #     for i in range(len(allconnectionlist)):
    #         try: 
    #             plot.draw_line([allconnectionlist[i].x, allconnectionlist[i].y, allconnectionlist[i].z], [allconnectionlist[i + 1].x, allconnectionlist[i + 1].y, allconnectionlist[i + 1].z], colours[colourcounter], ax)
    #         except: 
    #             break 
    
    # plt.show()