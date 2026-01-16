########################################
# Breadth first search algorithm
# 
# Does not work with netlist yet.
########################################
import csv
from mpl_toolkits import mplot3d
import numpy as np
import matplotlib.pyplot as plt

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
    ax.set_xlim3d(-1, size)
    ax.set_ylim3d(-1, size)

# Enter coordinates as list with: [X, Y, Z]
def draw_line(crdFrom, crdTo, colour):  
    Xline = [crdFrom[0], crdTo[0]]
    Yline = [crdFrom[1], crdTo[1]]
    Zline = [crdFrom[2], crdTo[2]]
    # Draw line
    print("LineFromTo",crdFrom , "To",crdTo, colour)
    ax.plot(Xline, Yline, Zline,lw=2,  color=colour, ms=12)

def set_gate(crd):
    PointX = [crd[0]]
    PointY = [crd[1]]
    PointZ = [crd[2]]
    # Plot points
    ax.plot(PointX, PointY, PointZ, ls="None", marker="o", color='red')


class Vertex():
    def __init__(self, name):
        self.name = name
        self.neighbours = []
        self.distance = 9999
        self.status = "unvisited"
    
    def add_neighbour(self, n):
        if n not in self.neighbours:
            self.neighbours.append(n)
            self.neighbours.sort()

class Graph():
    def __init__(self):
        self.vertices = {}


    def add_vertex(self, v):
        if isinstance(v, Vertex) and v.name not in self.vertices:
            self.vertices[v.name] = v
            return True
        else:
            return False
    
    def add_edge(self, i, j):
        if str(i) in self.vertices and str(j) in self.vertices:
            for key, value in self.vertices.items():
                if key == i:
                    value.add_neighbour(j)
                if key == j:
                    value.add_neighbour(i)
            return True
        else:
            return False

    def print_graph(self):
        for key in sorted(list(self.vertices.keys())):
            print(key + str(self.vertices[key].neighbours) + " " + str(self.vertices[key].distance))
    
    def path(self, goal):
        dist = self.vertices[goal].distance
        path = [goal]
        for i in self.vertices:
            for j in self.vertices[goal].neighbours:
                # Nog een random heuristiek
                if self.vertices[j].distance == dist - 1:
                    # print(self.vertices[j])
                    for k in self.vertices.items():
                        if self.vertices[j] == k[1]:
                            goal = str(k[0])
                            path.append(goal)

                    dist -= 1

        return path

    def bfs(self, node):
        queue = list()
        node.distance = 0
        node.status = "visited"
        for i in node.neighbours:
            self.vertices[i].distance = node.distance + 1
            queue.append(i)
        
        while len(queue) > 0:
            node1 = self.vertices[queue.pop(0)]
            node1.status = "visited"

            for i in node1.neighbours:
                node2 = self.vertices[i]
                if node2.status == "unvisited":
                    queue.append(i)
                    if node2.distance > node1.distance + 1:
                        node2.distance = node1.distance + 1


# Open file with netlist
data = open("../../data/example_net2.csv")

reader = csv.reader(data)

# Create netlist
netlist = []

for net_1, net_2 in reader:
    net = (net_1, net_2)
    netlist.append(net)

# Open file with gates

gates = open("../../data/example_prit2.csv")

reader = csv.reader(gates)

# Create list for gate coordinates
gate_coordinates = []

# Create list of coordinates that are not traversable
gates = []



fig = plt.figure()
ax = plt.axes(projection="3d")

make_grid(4, 4)


for number, x, y in reader:
    if x != " x":
        x = int(x)
        y = int(y)
        coordinates = [x, y, 0]
        gate_coordinates.append("(" + str(coordinates).strip("[]") + ")")

        # What to do? deepcopy? normal copy?
        gates.append(coordinates)



print("netlist: ", netlist)
print("gate crds: ", gate_coordinates)

# Convert list to tuple
def convert(list): 
    return (*list, ) 

def BFS_Algo(netlist, gate_coordinates):

    grid1 = []
    size = 5
    for x in range(-1, size):
        for y in range(-1, size):
            for z in range(size):
                grid1.append((x,y,z))


    allWires = {}


    

    wire = [] 

    blocked = []

    # For allWires keys
    tempCount = 0

    for net in netlist:
        if net[0] == "chip_a":
            continue
        start = str(gate_coordinates[int(net[0]) - 1])

        # niet meer nodig als ik het in het begin doe?
        # start = "(" + start.strip("[]") + ")"

        g = Graph()

        # Add start gate
        a = Vertex(start)
        g.add_vertex(a)


        for i in grid1:
            g.add_vertex(Vertex(str(i)))
            

        grid2 = []
        for i in grid1:
            grid2.append(i)
        
        edges = []

        for i in grid1:
            for j in grid2:
                if abs(j[0] - i[0]) == 1 and j[1] - i[1] == 0 and j[2] - i[2] ==0:    
                    if (j,i) not in edges:
                        edges.append((i,j))
                elif abs(j[1] - i[1]) == 1 and j[0] - i[0] == 0 and j[2] - i[2] == 0:
                    if (j,i) not in edges:
                        edges.append((i,j))
                elif abs(j[2] - i[2]) == 1 and j[0] - i[0] == 0 and j[1] - i[1] == 0:
                    if (j,i) not in edges:
                        edges.append((i,j))
        # print(edges)
        # for i in range(len(edges)):
        #     if (0, 3, 0) == edges[i][0] or (0, 3, 0) == edges[i][1]:
        #         edges.pop(i)
            # print(edges[i])
        
        

        end = str(gate_coordinates[int(net[1]) - 1])
        


        for i in gate_coordinates: 
            i = eval(i) 
            for j in edges:
                # if eval(i) in j:
                #     edges.remove(j)
                if i in j:
                    if i != eval(start) and i != eval(end):
                        edges.remove(j)
                    # for k in allWires:
                    #     for l in allWires[k]:
                    #         # print(allWires)
                    #         l = convert(l)
                    #         if l in j:
                    #             if j in edges:
                    #                 print("true", j)
                                    # edges.remove(j)
                    # else:
                    #     for k in allWires.items():
                            # print("!!!!!!: : :", k)



                else:
                    for k in blocked:
                        if k in j:
                            try:
                                edges.remove(j)
                            except:
                                pass


        # print(allWires, "allwiresss")
            # print("joejoe: ", j)

        for i in edges:
            g.add_edge(str(i[0]), str(i[1]))

        print()
        g.bfs(a)
        

        
        # end = "(" + end.strip("[]") + ")"
        # print(g.path(end))
        # if g.path(end)[0] == "(3, 1, 0)":
        #     print(edges) 
        
        for i in g.path(end):
            
            i = eval(i)
            if i != eval(start) and i != eval(end):
                blocked.append(i)
            wire.append(list(i))

        
        allWires[str(tempCount)] = wire
        tempCount += 1
        wire = []

        return allWires

for gate_coordinate in gates: 
    set_gate(gate_coordinate)
    plt.pause(0.03)


allWires = BFS_Algo(netlist, gate_coordinates)
print(allWires)

colours = ['b','lightgreen','cyan','m','yellow','k', 'pink']
colourcounter = 0 
# for i in range(len(allWires)):  


for keys in allWires:
    allConnections = allWires[keys]
    allconnectionlist = []
    for listconnection in allConnections: 
        allconnectionlist.append(listconnection)
    if colourcounter < 6:
        colourcounter += 1
    else: 
        colourcounter = 0
    for i in range(len(allconnectionlist)):
        try:
            # print("LineFromTo", allWires[i], "To",allWires[i + 1] )
            draw_line(allconnectionlist[i], allconnectionlist[i+1], colours[colourcounter] )
            plt.pause(0.05)
        except: 
            break    

ax.set_xlabel('x')
ax.set_ylabel('y')
ax.set_zlabel('z')

plt.show()

   
# g.print_graph()
# print(edges)
# print(len(edges))

print(len(allWires))