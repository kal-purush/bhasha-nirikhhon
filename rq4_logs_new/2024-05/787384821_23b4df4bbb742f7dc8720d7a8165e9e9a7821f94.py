import tkinter as tk
import random
from itertools import chain
from string import ascii_uppercase
import sys
from math import pi, cos, sin

sys.setrecursionlimit(3000)


class Node:
    def __init__(self, x, y, sign, neighbours=[]):
        self.x = x
        self.y = y
        self.radius = 18
        self.neighbours = neighbours
        self.sign = sign


class Edge:
    def __init__(self, node1, node2):
        self.node1 = node1
        self.node2 = node2


class GraphVisualizer:

    def __init__(self, root):
        self.root = root
        self.canvas = tk.Canvas(root, width=800, height=600, bg="white")
        self.canvas.pack()
        self.property_var = tk.StringVar()
        self.constructor()
        self.property_label = tk.Label(root, textvariable=self.property_var)
        self.property_label.pack(side=tk.TOP, anchor=tk.NW)
        self.canvas.bind("<Button-1>", self.constructor)

    def constructor(self, event=None):
        NODE_NUMBER = 7
        R = 200
        self.nodes = [
            Node(
                400 + R * cos(2 * pi / NODE_NUMBER * i),
                300 + R * sin(2 * pi / NODE_NUMBER * i),
                ascii_uppercase[i],
            )
            for i in range(NODE_NUMBER)
        ]
        for node in self.nodes:
            nd = self.nodes.copy()
            nd.remove(node)
            node.neighbours = random.choices(nd, k=random.randint(3, 6))
        self.cliques = []
        self.graph = {
            node.sign: set([nb.sign for nb in node.neighbours]) for node in self.nodes
        }
        # self.graph = {
        #     "A": {"B", "C"},
        #     "B": {"A", "C", "D"},
        #     "C": {"A", "B", "F"},
        #     "D": {"B", "E"},
        #     "E": {"D", "F"},
        #     "F": {"C", "E", "A"},
        # }
        # print(self.graph)
        self.bron_kerbosch(set(), set(self.graph.keys()), set())
        # print(self.cliques)
        self.redraw()
        self.property_var.set(self.cliques)

    def redraw(self):
        self.canvas.delete("all")
        for node in self.nodes:
            for nbr in node.neighbours:
                self.canvas.create_line(node.x, node.y, nbr.x, nbr.y, fill="black")
            x1, y1 = (node.x - node.radius), (node.y - node.radius)
            x2, y2 = (node.x + node.radius), (node.y + node.radius)
            self.canvas.create_oval(x1, y1, x2, y2, fill="gray")
            self.canvas.create_text(node.x, node.y, text=node.sign, font=("Arial", 14))

    def drawClique(self):
        colors = ["red", "blue", "green", "magenta", "brown"]
        max_l = len(max(self.cliques, key=len))
        max_cliques = list(filter(lambda x: len(x) == max_l, self.cliques))[:5]
        print(max_cliques)
        for i in range(len(max_cliques)):
            clique = set(max_cliques[i])
            for s in clique:
                for nb in clique | {s}:
                    node = self.findNode(s)
                    nbr = self.findNode(nb)
                    self.canvas.create_line(
                        node.x, node.y, nbr.x, nbr.y, fill=colors[i]
                    )
                    x1, y1 = (node.x - node.radius), (node.y - node.radius)
                    x2, y2 = (node.x + node.radius), (node.y + node.radius)
                    self.canvas.create_oval(x1, y1, x2, y2, outline=colors[i])

    def findNode(self, sign):
        for node in self.nodes:
            if node.sign == sign:
                return node
        return

    def bron_kerbosch(self, R, P, X):
        if not P and not X and len(R) > 2:
            self.cliques.append(list(R))
            # print(R)
        for v in P.copy():
            # print(R | {v}, P & self.graph[v], X & self.graph[v])
            self.bron_kerbosch(R | {v}, P & self.graph[v], X & self.graph[v])
            P.remove(v)
            X.add(v)