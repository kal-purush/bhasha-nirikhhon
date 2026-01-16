# -*- coding: utf-8 -*-

class Graph:
    """Representaion of a Graph of pages"""
    def __init__(self):
        self.graph = {}

    def addNode(self, name):
        """
        :param name: name of the Node
        """
        if self.graph.get(name) == None:
            self.update({name:{}})

    def addConnexions(self, start, end):
        """
        :param name: name of the starting node
        :param ends: name of the ending point or set of names
        """
        if type(end) == type(''):
            end = {end}
        if self.graph.get(start) == None:
            self.graph.update({start:end})
        else:
            a = self.graph.get(start)
            for el in end:
                a.add(el)
        self.addNode(x) for x in end

    def hasNode(self, node):
        """
        :param name: name of the Node
        :return: True if the graph has the Node
        """
        return self.graph.get(node) != None

    def hasEdge(self, start, end):
        """
        :param start: start point of the edge
        :param end: end point of the edge
        :return: True if the graph has the Edge
        """
        a = self.graph.get(start)
        return a != None and end in a

    def nodes(self):
        """
        :return: set of all Node
        """
        return {x for x in graph.keys()}

    def edges(self):
        """
        :return: set of all Node
        """
        ret = []
        for key in self.graph.keys():
            for val in self.graph.get(x):
                ret.append((key, val))
        return ret

#need to implement as much graph algorithm as possible (DFS, distance, etc ...)

