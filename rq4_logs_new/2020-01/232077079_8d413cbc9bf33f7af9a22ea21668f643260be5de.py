import csv
import random

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
print(gate_coordinates)


