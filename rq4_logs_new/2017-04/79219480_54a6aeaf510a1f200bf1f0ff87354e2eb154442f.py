#map coloring using constraint satisfaction
from collections import defaultdict as d
graph = d(list)
V = 4
number_of_colours = 3
index_of_colors = 4*[0]
def create_graph(x,y):
	graph[x].append(y)
def show():
	print graph

def isSafe(x,v):
	for i in len(graph[v]):
		if index_of_colors[i] == x:
			return False
		return True

def simulate(v):
	if v >= V:
		return True
	else:
		for x in range(number_of_colours):
			if(isSafe(x,v)):
				index_of_colors[v] = x

				if(simulate(v+1)):
					return True
				index_of_colors[v] = 0
		return False
create_graph(0,1)
create_graph(0,2)
create_graph(1,3)
create_graph(1,2)
create_graph(2,3)
simulate(0)
show()









