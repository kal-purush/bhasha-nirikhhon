from graph_bayes import graph
from math import log
import copy
import random

def get_data(filename = "./data/weather_data.txt"):
	"""Opens a file and reads in the data from that file
	
	The first line is expected to be the attributes of the training
	data.
	
	Args:
		filename: the file to be read
	
	Returns:
		data_types: The attributes of the training data
		data: the data from the file
	
	Raises:
		IOError: Failed to open the file
	"""
	file = open(filename)
	data_types = file.readline().split()
	data = []
	for line in file:
		data.append(line.split())
	return data_types, data

def revDict(dic):
	"""Reverses a dictionary (i.e turns keys to values and values to keys)
	
	Args:
		dic: The dictionary to be reversed
	
	Returns:
		res: the resulting dictionary
		
	Raises:
		TypeError: if the value corresponding to a key can't be made a key itself
	"""
	res = {}
	for key in dic:
		res[dic[key]] = key
	return res
	
def measure_graph(graph, data, data_types):
	"""Measures (logarithmically) the validity of a Bayesian network
	
	Args:
		graph: The graph to be examined
		data: The data from the experiments
		data_types: A list of the data types with a dict of their
					indices as the last element
	
	Returns:
		sum: the validity of the graph
	"""
	sum = 0
	rev = revDict(data_types[-1])
	for line in data:
		vals = line
		dic = {}
		i = 0
		for val in vals:
			dic[rev[i]] = val
			i += 1
		prob = gen_probs(graph, data,dic , data_types)
		sum += abs(log(prob))
	return sum

def get_parents(graph, vertex):
	"""Generates a list of the parents of a node on the graph
	
	Args:
		graph: the graph to examine
		vertex: the node whose parents are desired
		
	Returns:
		path: a list of the parents of the vertex
		
	Raises:
		ValueError: if the label given is not in the graph
	"""
	if vertex not in graph.edges:
		raise ValueError("Need a label in the graph")
	path = []
	for node in graph.edges:
		if vertex in graph.edges[node]:
			path.append(node)
	return path	
	
#desired == 
def gen_probs(graph, data, desired, data_types):
	"""Gets the probability of a desired data type in a desired value
	
	Args:
		graph: the graph to be examined
		data: the data from the input
		desired: a dict containg attributes and desired values
		data_types:A list of the data types with a dict of their
					indices as the last element
					
	Returns:
		result: the probability
	"""
	result = gen_prob(graph.primary, desired[graph.primary], data, data_types)
	for vertex in graph.edges:
		parents = get_parents(graph, vertex)
		desire = {}
		for parent in parents:
			desire[parent] = desired[parent]
		result *= gen_prob_multiple(data, data_types, vertex, desire)
	return result

#data == the data from the input
#data_types == a list whose last element is a dict matching the attributes to their index
#attribute == the node we're looking at
#desired == a dict such that the index is the value to be examined, and the value is the value desired
def gen_prob_multiple(data, data_types, attribute, desired, mu = float(1)):
	"""Calculates the probability of a desired data type in a desired value with parents
	
	Args:
		graph: the graph to be examined
		data: the data from the input
		attribute: the node we're looking at
		desired: a dict such that the index is the value to be examined, 
					and the value is the value desired
		data_types:A list of the data types with a dict of their
					indices as the last element
					
	Returns:
		result: the probability
	"""
	tot_vals = len(data)
	sum = 0
	for line in data:
		vals = line
		this_one = True
		for attribute in desired:
			if vals[data_types[-1][attribute]] != desired[attribute]:
				this_one = False
		if this_one:
			sum += 1
	result = float(sum + (mu/3))/float(tot_vals + mu)
	return result
	
	
def gen_prob(attribute, desired, data, data_types, mu = float(1)):
	"""Gets the probability of a desired data type in a desired value
	
	Args:
		graph: the graph to be examined
		data: the data from the input
		desired: the attributed to be examined
		data_types:A list of the data types with a dict of their
					indices as the last element
					
	Returns:
		result: the probability
	"""
	index = data_types[-1][attribute]
	tot_vals = len(data)
	sum = 0
	for line in data:
		vals = line
		if vals[index] == desired:
			sum += 1
	result = float(sum + (mu/3))/float(tot_vals + mu)
	return result
	
	
def build_edges(graph, data, data_types):
	"""Builds the possible graphs
	
	Args:
		graph: The graph to be examined
		data: The data from the experiments
		data_types: A list of the data types with a dict of their
					indices as the last element
	
	Returns:
		result_graph: the optimal graph found
	"""
	result_graph = graph.copyGraph()
	random.shuffle(result_graph.labels)
	#iterate over each vertex of the graph
	for vertex in result_graph.labels:
		#if the vertex is not the primary vertex
		if vertex != result_graph.primary:
			#iterate over all the other vertices
			#if we can add the edge to the graph do so
			#if the new graph is better accept it
			for p_child in result_graph.labels:
				#create a temperary graph
				temp_graph = result_graph.copyGraph()
				if (vertex not in temp_graph.edges[p_child]) and (p_child != vertex):
					temp_graph.addEdge(vertex, p_child)
				if abs(measure_graph(temp_graph, data, data_types)) > abs(measure_graph(result_graph, data, data_types)):
					result_graph = temp_graph.copyGraph()
	return result_graph

if __name__=="__main__":
	data_types, data = get_data()
	graphs = [graph(copy.deepcopy(data_types), primary = data_types[-1]) for _ in range(5)]
	
	dic = {}
	i = 0
	for type in data_types:
		dic[type] = i
		i += 1
	data_types.append(dic)
	
	final_graphs = [build_edges(g, data, data_types) for g in graphs]
	bst_graph = final_graphs[0]
	for graph in final_graphs[1:]:
		if abs(measure_graph(graph, data, data_types)) > abs(measure_graph(bst_graph, data, data_types)):
			bst_graph = graph
	bst_graph.printGraph()
	print("The measure of the final graph is {}".format(measure_graph(bst_graph, data, data_types)))
	