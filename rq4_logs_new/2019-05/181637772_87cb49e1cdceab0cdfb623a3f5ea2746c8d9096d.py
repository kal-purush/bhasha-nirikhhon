#!/usr/bin/env python

import os
import re
import argparse
import math
from shutil import rmtree

protocols = ['OLSR']  # ['AODV', 'OLSR', 'DSR', 'DSDV']
# Assume connection with gain=-10 should be 580
distance = 290

parser = argparse.ArgumentParser(description='construct connectivity matrices')
parser.add_argument('--results_path', help='The path to the results directory of the test', default='.')
parser.add_argument('--clean', default=False, action='store_true', help='Remove generated files')
args = parser.parse_args()


# Function generates a matrix of connections and returns what nodes the Gateway is connected to.
# The function takes a specific path to the test run such as path could be "AODV/AODV5/test5"
def matrix_generator(directory_name):
    file_name = directory_name + "/time"
    for files in os.walk(file_name):
        # Going through each file in the directory here
        
        for file in files[2]:
            # We read a file with an array of what nodes theoretically should be in communication distance
            with open('{}/{}'.format(file_name, file), 'r') as f:
                content = f.readlines()

            node_id = []
            x_coordinates = []
            y_coordinates = []
            for line in range(len(content)):
                real_content = [content[int(line)].rstrip()]

                for item in real_content:
                    entry = re.split(',', item)
                    node_id.append(entry[0])
                    x_coordinates.append(entry[1])
                    y_coordinates.append(entry[2])

            connectivity_matrix = []

            # In this code below the distance between nodes is checked and when
            # the distance is less than the Distance variable defined previously
            # The node will be set as "available" for communication formula for
            # distance is sqrt((Xi - Xj)^2 + (Yi - Yj)^2)
            # Where Xi, Yi and Xj, Yj are the coordinates for a specific node at the time.
            # In this case for a specific row Xi,Yi is defined and compared to
            # all other entries on that row to check if they can communicate.
            # This will go on until all nodes have been checked through.

            # after running this through a symmetric matrix is created which will indicate what nodes are connected.
            # Where all nodes are connected to themselves

            for i in node_id:
                connectivity_row = []
                for k in range(len(node_id)):
                    connectivity_row.append(0)
                for j in node_id:
                    local_distance = math.sqrt((float(x_coordinates[int(i)]) - float(x_coordinates[int(j)]))**2
                                               + (float(y_coordinates[int(i)]) - float(y_coordinates[int(j)]))**2)

                    if local_distance < distance:
                        connectivity_row[int(j)] = 1
                    else:
                        connectivity_row[int(j)] = 0
                connectivity_matrix.append(connectivity_row)

            if not os.path.isdir('{}/Matrix'.format(directory_name)):
                os.makedirs('{}/Matrix'.format(directory_name))

            with open('{}/Matrix/connectivitymatrix{}'.format(directory_name, file), 'w') as f:
                for row in connectivity_matrix:
                    string = str(row)[1:-1]
                    f.write(string + '\n')
                    

# Simple way to automatically call the MatrixGenerator for all different simulations run
def generate_neighbour_matrix():
    for protocol in protocols:
        # Protocols here are AODV, OLSR, DSR, DSDV which are defined previously
        # protocol="OLSR"
        for node_count in os.walk(protocol).next()[1]:
            if os.path.isdir('{}/{}'.format(protocol, node_count )):
                for run in os.walk("{}/{}" .format(protocol, node_count)).next()[1]:
                    directory_name = protocol + "/" + node_count + "/" + run
                    matrix_generator(directory_name)


# This function takes the results from Matrixgenerator and check how well connected the network is
def connectivity(directory_path, file):
    # First a specific matrix is read and will be processed
    neighbour_list = []
    with open('{}/{}'.format(directory_path, file), 'r') as f:
        lines = f.readlines()
        for line in lines:
            neighbour_list.append(line.rstrip().split(","))

    node_check = []
    for i in range(len(neighbour_list)):
        node_check.append(True)
    node_check[0] = False
    connection = neighbour_list[0]

    connection = [x for x in connection if x]
    connection = [float(k) for k in connection]

    # Example of how an array may look and the logic
    #                   1,0,0,1,0
    #                   0,1,1,1,0
    #                   0,1,1,0,0
    #                   1,1,0,1,1
    #                   0,0,0,1,1
    # On the first row a check is done for what neighbours the gateway has. As it has the neighbour 4 it
    # may add 4's neighbour list to its own which means the first row is now:
    #                   2,1,0,2,1
    # Which means it now can also add node 2 and 5's neighbour to its own.
    # this will go on until all neighbour's lists are added
    # After adding all possible neighbours all numbers that aren't 0
    # will be set to 1 and afterwards summed up and divided by the amount of nodes
    # As this will define the how much well connected the network is.
    # in order to ensure that a network with only node 1 part of it doesn't have
    # a connectivity level 1 is substracted from the sum and from the amount of nodes which will be divided by.
    # The node_check array previously mentioned is to ensure that a nodes neighbour list is only added once
    # to ensure infinite loop won't occur the "loop" variable is defined
    loop = True
    while loop:
        loop = False
        for i in range(len(connection)):
            if node_check[i] and connection[i] > 0:
                loop = True
                for k in range(len(connection)):
                    value = neighbour_list[int(i)]
                    connection[k] = float(connection[k]) + float(value[k])
                node_check[i] = False
                break
    for i in range(len(connection)):
        if int(connection[i]) != 0:
            connection[i] = 1
    print("Vi er {}/{} ".format(directory_path, file))
    print(connection)

    connection_level = ((sum(connection)-1)*1.0/(len(connection)-1)*1.0)

    with open("{}/connection_level.txt".format(directory_path), 'a') as f:
        f.write("{},".format(connection_level))


# comments applied to the generate_neighbour_matrix() are also applied here
# This function will call the connectivity function for all different simulations
# It has to be called after generate_neighbour_matrix() function
def generator_connectivity():
    for protocol in protocols:
        for node_count in os.walk(protocol).next()[1]:
            if os.path.isdir('{}/{}'.format(protocol, node_count)):
                for run in os.walk("{}/{}".format(protocol, node_count)).next()[1]:
                    directory_name = protocol + "/" + node_count + "/" + run + "/Matrix"
                    for file in os.walk("{}/{}/{}/Matrix".format(protocol, node_count, run)).next()[2]:
                        connectivity(directory_name, file)


# function which will collect information generated by the other functions
# It will average out the connectivity level for a simulation type i.e 5nodes
# This i done by averaging the connectivity of a single simulation
# After which it will averaged for all simulation using the same parameters
# such as 5nodes, 10 nodes and different routing
# routing should however not affect this at all but it is still calculated.
def combine_data():
    for protocol in protocols:
        for node_count in os.walk(protocol).next()[1]:
            print("test testes der for ")

            total = 0
            if not os.path.isdir('{}/{}'.format(protocol, node_count )):
                for run in os.walk("{}/{}".format(protocol, node_count)).next()[1]:
                    # We find a directory such as "AODV/AODV5/test5/Matrix" which contains connectionlevel.txt
                    # this file contains all the different connectivity levels throughout the simulation.
                    for file in os.walk("{}/{}/{}/Matrix" .format (protocol, node_count, run)).next()[2]:
                        if file == "connectionlevel.txt":
                            
                            data_content
                            with open("{}/{}/{}/Matrix/connectionlevel.txt".format(protocol, node_count, run)) as f:
                                content = f.read()
                                data_content = re.split(',', content)
                                
                            total = 0
                            # We loop through the file and read all the values and add them together and average them
                            for i in range(len(data_content)-1):
                                # Speciel case where 0.00000 is problematic to work with
                                if data_content[i] != "0.000000":
                                    total = float(total) + float(data_content[i])

                            # After running an one of the connectionlevel.txt files through,
                            # its averaged using the amount of entries
                            connectivity_per_simulation = total/len(data_content)
                            # This is added to "samlet" which will contain all values using same parameters
                            total = total + connectivity_per_simulation
                # Average connectivity for simulaton with same parameters
                total = total/len(os.walk("{}/{}" .format(protocol, node_count)).next()[1])
                print(total)
                with open("Connectivity.txt", 'a') as f:
                    f.write(node_count + "{}\n{}\n".format(node_count, total))


def clean():
    for protocol in protocols:
        for node_count in os.walk(protocol).next()[1]:
            if os.path.isdir('{}/{}'.format(protocol, node_count )):
                for run in os.walk("{}/{}" .format(protocol, node_count)).next()[1]:
                    directory_name = '{}/{}/{}/Matrix'.format(protocol, node_count, run)
                    print('Removing ' + directory_name)
                    rmtree(directory_name)


os.chdir(args.results_path)

if args.clean:
    clean()
    exit(0)

try:
    clean()
except OSError:
    pass

generate_neighbour_matrix()
generator_connectivity()
combine_data()














