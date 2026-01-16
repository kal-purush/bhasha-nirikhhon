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
    vertices = {}

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


grid1 = []
for x in range(3):
    for y in range(3):
        for z in range(3):
            grid1.append((x,y,z))

g = Graph()
a = Vertex("(0, 0, 0)")
g.add_vertex(a)


for i in grid1:
    g.add_vertex(Vertex(str(i)))
    

grid2 = []
grid3 = []
for i in grid1:
    grid2.append(i)
    grid3.append(i)

edges = []
print(grid1)
# for i in grid1:
#     for j in grid2:
#         for k in grid3:
#             if abs(j[0] - i[0] - k[0]) == 1 and j[1] - i[1] - k[1] == 0 and k[2] - j[2] - i[2] == 0:
#                 if (i,j,k) not in edges:
#                     edges.append((i,j,k))
#             elif abs(j[1] - i[1] - k[1]) == 1 and j[0] - i[0] - k[0] == 0 and k[2] - j[2] - i[2] == 0:
#                 if (i,j,k) not in edges:
#                     edges.append((i,j,k))
#             elif abs(j[2] - i[2] - k[2]) == 1 and j[0] - i[0] - k[0] == 0 and k[1] - j[1] - i[1] == 0:
#                 if (i,j,k) not in edges:
#                     edges.append((i,j,k))

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
print("#@ edges: ", len(edges))

# for i in edges:
#     g.add_edge(str(i[0]), str(i[1]), str(i[2]))

# edges = ["(0, 0, 0) (0, 1, 0)", "(0, 0, 0) (1, 0, 0)", "(0, 0, 0) (0, 0, 1)", "(1, 0, 0) (1, 1, 0)", "(1, 0, 0) (1, 0, 1)", "(0, 1, 0) (0, 1, 1)", "(0, 1, 0) (1, 1, 0)", "(1, 1, 0) (1, 1, 1)"]
for i in edges:
    g.add_edge(str(i[0]), str(i[1]))

print()




print(g.vertices)


g.bfs(a)
g.print_graph()        

    