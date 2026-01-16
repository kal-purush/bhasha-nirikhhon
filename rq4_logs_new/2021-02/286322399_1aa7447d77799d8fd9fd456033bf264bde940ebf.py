class Edge:
    def __init__(self, other_node, weight):
        self.other_node = other_node
        self.weight = weight

with open("1.in", "r") as input_file:
    line1 = input_file.readline().split()
    N = line1[0]
    Q = line1[1]
    edge_listOfList = [] #index of this list is a node
    for i in range(N):
        line2 = input_file.readline().split()
        x = line2[0]
