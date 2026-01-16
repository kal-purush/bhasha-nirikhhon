import networkx as nx
from collections import defaultdict
from collections import deque
import random

G = nx.Graph()

# CREATING GRAPH FROM DATASET
with open('USA_dataset.txt') as f:
    count = 0
    for lines in f:
        count += 1
        line = lines.split()
        if len(line) < 4:
            continue
        G.add_node(int(line[1]), L=[])
        G.add_node(int(line[2]), L=[])
        G.add_edge(int(line[1]), int(line[2]), w=int(line[3]))
    print(count)

print(G)

# Create Hub Labellings


for i in G:
    G.nodes[i]["L"] = G.nodes[i]["L"] + [[i, 0]]

marked = set()  # has nodes that are already traversed
visited = set()

# BFS
def BFS(i):
    global visited
    if i in visited:
        return
    else:
        visited.add(i)
    q = deque()
    q.append(i)

    while len(q) != 0:  # run until q is empty
        i = q.popleft()
        lst = list(G.nodes[i]["L"])
        for j in G[i]:
            wt = G[i][j]['w']
            lst2 = list()
            for t in lst:
                lst2.append([t[0], t[1] + wt])
            if j not in visited:
                q.append(j)
                visited.add(j)
                for l1 in lst2:
                    for l2 in G.nodes[j]["L"]:
                        if l2[0] == l1[0]:
                            if l2[1] > l1[1]:
                                G.nodes[j]["L"] += [l1]
                                G.nodes[j]["L"].remove(l2)
                            if l1 in lst2:
                                lst2.remove(l1)
                G.nodes[j]["L"] += lst2


for node in G.nodes:
    BFS(node)

print(G.nodes[245759]["L"])

# #Query two nodes to receive distance

# #Define a class

# #class RoadNetwork
#
# This class can be used to initialize hotels and query distance for the functions of the algorithm
#
# There are two functions
#
# def get_distance(self,a,b):
#
# >This function receives two integer node values and returns the minimum distance between them
#
# def get_hotels(self):
#
# >This function initializes a random set of nodes as hotels and orders are delivered from hotels.
#


class DSU:
    def __init__(self, nodes):
        self.parent = dict()
        for node in nodes:
            self.parent[node] = node

    def find(self, x):
        if self.parent[x] == x:
            return x
        self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        px = self.find(x)
        py = self.find(y)
        if px == py:
            return False
        self.parent[px] = py
        return True


class RoadNetwork:

    def __init__(self):
        self.hotels = self.generate_hotels()
        self.max_vehicle_size = 3
        self.number_of_hotels = len(self.hotels)

    def get_distance(self, a, b):
        ulst = G.nodes[a]["L"]
        vlst = G.nodes[b]["L"]

        distance = 9999999999
        if a != b:
            for l1 in ulst:
                for l2 in vlst:
                    if l1[0] == l2[0]:
                        distance = min(distance, l1[1] + l2[1])
        return distance

    def generate_hotels(self):
        hotels = list()
        length = len(G.nodes())
        x = int(0.01 * length)
        y = int(0.05 * length)
        hotelscount = random.randrange(x, y)
        # print(hotelscount)
        for i in range(hotelscount):
            hotels.append(int(random.random() * length))
        return hotels

    def get_hotels(self):
        return self.hotels

    def get_random_orders(self, size):
        orderlist = []
        length = len(G.nodes())
        while len(orderlist) < size:
            val = int(random.random() * length)
            if val not in self.hotels:
                hot = int(random.choice(self.hotels))
                orderlist.append((val, hot))
        return orderlist

    def get_path_cost_for_nodes(self, nodes):
        edges = []
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                edges.append([nodes[i], nodes[j], self.get_distance(nodes[i], nodes[j])])
        edges.sort(key=lambda x: x[2])
        dsu = DSU(nodes)
        total = 0
        for edge in edges:
            if dsu.union(edge[0], edge[1]):
                total += edge[2]
        return total


def inform(optimal_matching):
    # TODO  : have a reference to graph object and update nodes and orders
    print("Optimal matching found !!!")
    for cluster in optimal_matching:
        print(cluster)


class Cluster:
    def __init__(self, __nodes, initial_cost):
        self.nodes = __nodes
        self.cost = initial_cost

    def __str__(self):
        ret = '{ Nodes : '
        ret += str(self.nodes)
        ret += ','
        ret += 'Total Cost : ' + str(self.cost) + ' }'
        return ret

    def merge(self, other_cluster, new_cost):
        for node in other_cluster.nodes:
            self.nodes.append(node)
        self.cost = new_cost

    def add_node(self, x):
        self.nodes.append(x)


class FoodGraph:
    def __init__(self, extRef, shop_orders, restaurant):
        self.clusters = []
        self.delta = 2 ** 70
        self.start = restaurant
        self.ref = extRef
        self.max_size = self.ref.max_vehicle_size
        self.max_iter = 10
        for order in shop_orders:
            self.clusters.append(Cluster([order], self.ref.get_distance(restaurant, order)))
        self.converge()

    def converge(self):
        while self.max_iter != 0 and self.get_average_cost() < self.delta:
            if not self.relax():
                break
            self.max_iter -= 1
        inform(self.clusters)

    def relax(self):
        # cluster is a set of nodes and cost(i)
        best = [-1, -1, 2 ** 70, -1]
        for i in self.clusters:
            for j in self.clusters:
                if i == j or len(i.nodes) + len(j.nodes) > self.max_size:
                    continue
                # calculate w(i , j) = c(i , j) - c(i) - c(j)
                r = self.ref.get_path_cost_for_nodes([self.start] + i.nodes + j.nodes)
                w = r - i.cost - j.cost
                if w <= best[2]:
                    best = [i, j, w, r]
        if best[2] == 2 ** 70:
            return False
        # modify the clustering
        self.clusters.remove(best[0])
        self.clusters.remove(best[1])
        # construct a new cluster
        best[0].merge(best[1], best[3])
        self.clusters.append(best[0])
        return True

    def get_average_cost(self):
        total = 0
        cnt = 0
        for cluster in self.clusters:
            total += cluster.cost
            cnt += len(cluster.nodes)
        return total / cnt


class Solver:
    def __init__(self, ext_Ref):
        self.order_window = ext_Ref.get_random_orders(20)
        self.matching = []
        self.ref = ext_Ref  # reference to the network object ...
        self.simulate()

    def simulate(self):
        # put an order in the corresponding shop bucket
        orders_of_shop = defaultdict(list)
        for order in self.order_window:
            orders_of_shop[order[1]].append(order[0])
        for key in orders_of_shop.keys():
            print("\nsolving for hotel ", key)
            fg = FoodGraph(self.ref, orders_of_shop[key], key)

    def query(self, x, y):
        return self.ref.get_distance(x, y)


solver = Solver(RoadNetwork())