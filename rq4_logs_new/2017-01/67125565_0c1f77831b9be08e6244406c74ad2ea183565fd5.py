class GraphMat:
    def __init__(self, order, directed = False):
        self.order = order
        self.directed = directed
        self.adj = [[0 for j in range(order)] for i in range(order)]

class Graph:
	def __init__(self, order, directed = False):
		self.order = order
		self.directed = directed
		self.adjLists = []
		for i in range(order):
		    self.adjLists.append([])