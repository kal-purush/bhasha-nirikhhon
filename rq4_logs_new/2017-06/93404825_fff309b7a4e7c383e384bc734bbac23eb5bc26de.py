#!/usr/bin/env python

"""Network graph library
"""

#import essential modules, libraries and methods/functions
from math import sqrt

"""Define classes of this library
"""

#network graph point/vertice class
class Vertice(object):
    """Vertice to be used in a network graph. Attributes: x and y coordinates.
    """
    def __init__(self, x=0, y=0):
        self.x = x
        self.y = y

    def __str__(self):
        return "Vertice with coordinates: %d / %d" % (self.x, self.y)

    def id(self):
        return str(0) + str(self.x) + str(self.y)

class Edge(object):
    """Edge to be used in a network graph. Attributes: start, end as Vertice objects.
    """
    def __init__(self, start=Vertice(0,0), end=Vertice(1,1)):
        self.start = start
        self.end = end
        self.sort_coordinates()

    def __str__(self):
        return "Edge with vertices at %d / %d and %d / %d." % (self.start.x, self.start.y, self.end.x, self.end.y)

    def length(self):
	return sqrt((self.start.x - self.end.x) ** 2 + (self.start.y - self.end.y) ** 2)

    def id(self):
        return str(9) + str(self.start.x) + str(self.start.y) + str(self.end.x) + str(self.end.y)

    def sort_coordinates(self):
        if self.start.x > self.end.x:
            self.start, self.end = (self.end, self.start)


class Graph(object):
    """Graph to be populated by Vertice and Edge objects.
    """
    def __init__(self, vertices={}, edges={}):
        self.vertices = vertices
        self.edges = edges

    def __str__(self):
        return "Graph with vertices %s and edges %s." % (self.vertices, self.edges)

    def add_edge(self,edge):
        self.edges[edge.id()] = edge
        self.add_vertice(edge.start)
        self.add_vertice(edge.end)

    def add_vertice(self, vertice):
        self.vertices[vertice.id()] = vertice

vert1 = Vertice(2,5)
print vert1
vert1.id()
edge1 = Edge(vert1)
print edge1.length()
print edge1
graph=Graph()
graph.add_edge(edge1)
print graph