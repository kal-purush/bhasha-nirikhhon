########################################
# Breadth first search algorithm
# 
# Does not work with netlist yet.
########################################



import csv


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
data = open("/home/izharhamer/mprog-theorie/Chips-Circuits/Unordered code first week/example_net2.csv")
reader = csv.reader(data)

# Create netlist
netlist = []

for net_1, net_2 in reader:
    net = (net_1, net_2)
    netlist.append(net)

# Open file with gates
gates = open("/home/izharhamer/mprog-theorie/Chips-Circuits/Unordered code first week/example_prit2.csv")
reader = csv.reader(gates)

# Create list for gate coordinates
gate_coordinates = []

# Create list of coordinates that are not traversable
blocked = []

for number, x, y in reader:
    if x != " x":
        x = int(x)
        y = int(y)
        coordinates = [x, y, 0]
        gate_coordinates.append("(" + str(coordinates).strip("[]") + ")")

        # What to do? deepcopy? normal copy?
        blocked.append("(" + str(coordinates).strip("[]") + ")")



print("netlist: ", netlist)
print("gate crds: ", gate_coordinates)

grid1 = []
size = 4
for x in range(size):
    for y in range(size):
        for z in range(size):
            grid1.append((x,y,z))



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
                    # if str(i) not in blocked and str(j) not in blocked:
                    #     edges.append((i,j))
                    edges.append((i,j))
            elif abs(j[1] - i[1]) == 1 and j[0] - i[0] == 0 and j[2] - i[2] == 0:
                if (j,i) not in edges:
                    # if str(i) not in blocked and str(j) not in blocked:
                    #     edges.append((i,j))
                    edges.append((i,j))
            elif abs(j[2] - i[2]) == 1 and j[0] - i[0] == 0 and j[1] - i[1] == 0:
                if (j,i) not in edges:
                    # if str(i) not in blocked and str(j) not in blocked:
                    #     edges.append((i,j))
                    edges.append((i,j))
    
    # for i in range(len(edges)):
    #     if (0, 3, 0) == edges[i][0] or (0, 3, 0) == edges[i][1]:
    #         edges.pop(i)
        # print(edges[i])
    
    for i in edges:
        if (3, 1, 0) in i:
            edges.remove(i)

    
    for i in edges:
        g.add_edge(str(i[0]), str(i[1]))

    print()



    g.bfs(a)
     

    end = str(gate_coordinates[int(net[1]) - 1])
    # end = "(" + end.strip("[]") + ")"
    print(g.path(end))     

# g.print_graph()
print(edges)
print(len(edges))