import numpy
import itertools
import shapely
from shapely.geometry import Polygon
from shapely.geometry import LineString
from shapely.geometry import Point
import queue as Q
import pdb
import matplotlib.pyplot as plt



class NodeState(object):
	# priority = 999999
	path = []
	goal_co_ords = [200,30]
	parent = 0
	# dist = 0

	def __init__(self, id, co_ords, links = []):
		self.id = id
		self.children = []
		# self.parent = parent
		self.co_ords = co_ords
		self.links = links[:]
		self.priority = 99999
		self.dist = 0
		self.goal_co_ords = [200,30]

	def calc_dist(self, origin, destination):
		d = ((destination[0] - origin[0])**2 + (destination[1] - origin[1])**2)**0.5
		return d

	def update_dist(self, parent):
		if parent:
			self.parent = parent
			self.dist = parent.dist + self.calc_dist(self.parent.co_ords, self.co_ords)
		else:
			self.dist = 0

	def update_priority(self):
			self.priority = self.dist + self.calc_dist(self.co_ords, self.goal_co_ords)
			# self.priority = self.calc_dist(self.co_ords, self.goal_co_ords)


	def get_priority(self):
		return self.priority

	def get_id(self):
		return self.id

	def update_path(self, parent):
		self.path = parent.path[:]
		self.path.append(self.co_ords)

	def update_links(self, node_list, obstruction_list):
		for node in node_list:
			if not self.co_ords == node.co_ords:
				line_attempt = LineString([self.co_ords, node.co_ords])
				success = True
				for obstruction in obstruction_list:
					if line_attempt.crosses(obstruction.polygon):
						success = False
						break
				if success == True:
					self.links.append(node.id)
						
				


	# def get_priority(elem):
	# 	return elem.priority

	def create_children(self, nodes):
		if not self.children and self.links:
			self.nodes = nodes
			for x in self.links:
				# print 'aafkjsdlfk'
				child = self.nodes.get_node(x)
				child.parent = self
				child.update_dist(self)
				child.update_priority()
				child.update_path(self)
				# print 'Child Created: Node' + str(child.id)
				# print ('with details', str(child))

				# print str(child.id)
				self.children.append(child)
			# self.children.sort(key=get_priority())

	def __str__(self):
		return "[Node" + str(self.id) + "," + str(self.co_ords) + ", priority " + str(self.priority) +  "," + str(self.dist) + "," + str(self.links) + "]"

			# for i in xrange(len(self.goal)-1):
			# 	val = self.priority
			# 	val = val[:i] + val[i+1] + val[i] + val[i+2]
			# 	child = State_String(val, self)
			# 	self.children.append(child)

# class SateString(state):
# 	def __init__(self, id, co_ords, priority, parent, distance, links):
# 		super(State_String, self).__init__(id, co_ords, priority, parent, distance, links)
# 		self.distance = self.GetDist()

# 	def get_dist(self):
# 		# if self.co_ords == self.goal:
# 		# 	return 0
# 		dist = parent.getdist + ((self.co_ords[0]-parent.co_ords[0])**2+(self.co_ords[1]-parent.co_ords[1])**2)**(1/2.0)
# 		return dist

class NodeList(list):
	def __init__(self):	
		self.append(NodeState(0, (10, 75)))
		self.append(NodeState(1, (10, 150)))
		self.append(NodeState(2, (10, 225)))
		self.append(NodeState(3, (590, 75)))
		self.append(NodeState(4, (590, 150)))
		self.append(NodeState(5, (590, 225)))
		self.count = 6

	def get_node(self, id):
		for j in self:
			if j.id == id:
				return j

	def print_list(self):
		print ('[%s]' % '\n '.join(map(str, self)))

	def add_node(self, node_point, id = 0):
		# for node in block.block_nodes
		if not id:
			new_id = self.count
			self.count += 1
		else:
			new_id = id
		new_node = NodeState(new_id, node_point)
		# print(str(new_node))
		self.append(new_node)
		# print(str(self.node_list))
		


class AStarSolver:

	def __init__(self, start, goal, nodes, obstruction_list):
		self.path = []
		self.visited_queue = []
		self.priority_queue = Q.PriorityQueue()
		self.start = start
		self.goal = goal
		self.nodes = nodes
		self.obstruction_list = obstruction_list
		self.dist = 0
		self.nodes.add_node(start.co_ords, 7000)
		self.nodes.add_node(goal.co_ords, 7001)
		# self.nodes.print_list()

		for node in self.nodes:
			node.update_links(self.nodes, self.obstruction_list)
			node.goal_co_ords = self.goal.co_ords
		print("Node List with updated links")
		self.nodes.print_list()
		# pdb.set_trace()

	def print_impossible(self):
		print ("Goal of " + str(self.goal.co_ords) + "is not possible!")



	def solve(self):
		start_state = self.start
		start_state.distance = 0
		start_state.priority = 0
		# start_state = NodeState(0, self.start, 0, 0, 0, [1, 2])
		count = 0
		self.priority_queue.put((0, count, start_state))
		while (not self.path and not self.priority_queue.empty()):
			closest_child = self.priority_queue.get()[2]
			print ('closest child is Node:' + str(closest_child.id)) 
			print('with details', str(closest_child))
			closest_child.create_children(self.nodes)
			print ('Children of Closest Child are' + '[%s]' % ', '.join(map(str, closest_child.children)))
			# print str(closest_child.children)
			self.visited_queue.append(closest_child.id)
			print ('Nodes visited so far: ' + str(self.visited_queue))
			print (' Distance travelled so far: ' + str(closest_child.dist))
			for child in closest_child.children:
				if child.calc_dist(child.co_ords, self.goal.co_ords) == 0.0:
					self.path.append(self.start.co_ords)
					self.path.extend(child.path[:])
					self.dist = child.dist
					break
				if child.id not in self.visited_queue:
					count += 1
					# self.priority_queue.put((child.priority, count, child))
					self.priority_queue.put((child.priority, count, child))



		if not self.path:
			self.print_impossible
		print ('HOORAY ROBBIE! YOURE THE GREATEST!!! Final Path: ' + str(self.path))
		print ('Total Distance Travelled: ' + str(self.dist))
		return self.path

# class Point:
# 	def __init__(self,x,y):
# 		self.x = x
# 		self.y = y

# def ccw(A,B,C):
# 	return (C.y-A.y)*(B.x-A.x) > (B.y-A.y)*(C.x-A.x)

# def intersect(A,B,C,D):
# 	return ccw(A,C,D) != ccw(B,C,D) and ccw(A,B,C) != ccw(A,B,D)

class Block(object):
	"""A class for the foam blocks"""
	board = [600, 300]
	boundaries = [[[0,0],[600,0]],[[600,0],[600,300]],[[600,300],[0,300]],[[0,300],[0,0]]]

	def __init__(self, centroid, vertices):
		self.centroid = centroid
		self.vertices = vertices
		self.polygon = Polygon(self.vertices)

	# def expand_bbox(self, radius):
	# 	"""Expands the bounding box of a block by the radius of the end effector"""
	# 	new_vertices = []
	# 	for i in self.vertices:
	# 		# new_x = i[0] + (self.centroid[1]-i[1]/self.centroid[0]-i[0])*radius
	# 		new_x = i[0] + numpy.sign(i[0]-self.centroid[0])*radius
	# 		new_y = i[1] + numpy.sign(i[1]-self.centroid[1])*radius
	# 		# new_y = i[1] + radius
	# 		new_vertex = [new_x, new_y]
	# 		new_vertices.append(new_vertex)
	# 	self.vertices = new_vertices

	def __str__(self):
		"""Returns the string form of Block"""
		return str(self.centroid) + str(self.vertices)

	# def get_edges(self):
	# 	"""	Creates and returns a list of edges for the block bounding box"""
	# 	self.edges = []
	# 	for i in range(0, len(self.vertices) -1):
	# 		new_edge = [self.vertices[i], self.vertices[i+1]]
	# 		self.edges.append(new_edge)
	# 	new_edge = [self.vertices[-1], self.vertices[0]]
	# 	self.edges.append(new_edge)
	# 	# return list(self.edges)
	# 	return self.edges

	def cell_decomp(self, obstruction_list):
		"""Performs trapezoidal cell decomposition for individual block"""
		self.block_nodes = []
		self.obstrucion_list = obstruction_list
		# self.bbox = box(self.polygon.bounds)

		for i in self.polygon.exterior.coords:
			new_line = LineString([i, (i[0], 0)])
			if new_line.crosses(self.polygon):
				new_line = LineString(i, (i.x, 300))
			for j in obstruction_list:
					if new_line.crosses(j.polygon):
						new_line = LineString([i, (new_line.intersection(j.polygon.boundary))[0]])
			node_coords = new_line.interpolate(0.5, normalized=True).coords[:][0]
			# print ("node created at " + str(node_coords))	
			self.block_nodes.append(node_coords)
		# print ("block nodes created:- " + str(self.block_nodes))
		return self.block_nodes
		


		# for i in range(0,1):
		# 	new_line = lineString(self.polygon.bounds.xmin, (self.bbox[i].x, 0))
		# 	# new_line = [[self.vertices[x], [self.vertices[x][0], 0]]
		# 	for edge in self.obstrucion_edges:
		# 		if new_line intersects
		# 		new_line = [[self.vertices[x], intersecting point of line]
		# 	node_co_ords = 	[self.vertices[x][0], average (self.vertices[x][1], intersecting point of line[1]]
		# self.block_nodes.append(node_co_ords)

		# for x in range(2,3):
		# 	new_line = [[self.vertices[x], [self.vertices[x][0], 300]]
		# 	for lines in global_lines:
		# 		if new_line intersects
		# 		new_line = [[self.vertices[x], intersecting point of line]
		# 	node_co_ords = 	[self.vertices[x][0], average (self.vertices[x][1], intersecting point of line[1]]
		# self.block_nodes.append(node_co_ords)
		




class Scene:
	'''A class to model the scene on a workspace'''
	end_effector_r = 5
	holding_spot = [350, 300]

	def __init__(self, pucks, targets, immovable_blocks = 0, movable_blocks = 0):
		"""initialises a scene ready for solving"""

		self.pucks = pucks
		self.targets = targets
		self.immovable_blocks = immovable_blocks
		self.movable_blocks = movable_blocks
		self.obstruction_list = []
		self.edges = []
		self.node_list = NodeList()

	def expand_obstructions(self):
		for blocks in self.immovable_blocks:
			blocks.polygon.buffer(self.end_effector_r)
		for blocks in self.movable_blocks:
			blocks.polygon.buffer(self.end_effector_r)
		self.obstruction_list.extend(self.immovable_blocks)
		self.obstruction_list.extend(self.movable_blocks)

	# def get_edges(self)
	# 	for block in self.immovable_blocks:
	# 		new_edges = block.get_edges()
	# 		self.edges.extend(new_edges)
	# 	for block in self.movable_blocks:
	# 		new_edges = block.get_edges()
	# 		self.edges.extend(new_edges)

	def create_nodes(self):
		for block in self.immovable_blocks:
			nodes = block.cell_decomp(self.obstruction_list)
			# print (nodes)
			# self.node_list.extend(nodes)
			# print (self.node_list)
			for node in nodes:
				self.node_list.add_node(node)
				# print("Node from immovable_blocks added" + str(node))
		for block in self.movable_blocks:
			nodes = block.cell_decomp(self.obstruction_list)
			# print (nodes)
			# self.node_list.extend(nodes)
			for node in nodes:
				self.node_list.add_node(node)
		# self.node_list.extend(self.node_list)

	def plot_nodes(self):
		xlist = []
		ylist = []
		for node in self.node_list:
			xlist.extend(node.co_ords)
			ylist.extend(node.co_ords)

		plt.plot(xlist, ylist, 'ro')
		plt.axis([0, 600, 0, 300])
		plt.show()

	def solve_puck_paths(self):
		solution = AStarSolver(self.pucks[0], self.targets[0],self.node_list, self.obstruction_list)
		self.puck_path = solution.solve()

	def solve_initial_path(self):
		solution = AStarSolver(self.holding_spot, self.pucks[0], self.node_list, self.obstruction_list)
		self.initial_path = solution.solve()

	def solve_return_path(self):
		solution = AStarSolver(self.targets[0], self.holding_spot, self.node_list, self.obstruction_list)
		self.initial_path = solution.solve()






# end_effector_r = 5

block1 = Block(Point(62,25), [(50,17),(70,17),(70,32),(50,32)])
# print (str(block1))
block2 = Block([75,62], [[65,51],[85,51],[85,72],[65,72]])
# print (str(block2))
block3 = Block([134,35], [[124,24],[144,24],[144,44],[124,44]])
# print (str(block3))
puck1 = NodeState(7000, [12, 57], [1,2])
target1 = NodeState(7001, [200,30], [11,12])

i_blocks = [block1, block2]
m_blocks = [block3]
pucks = [puck1]
targets = [target1]


scene = Scene(pucks, targets, i_blocks, m_blocks)
scene.expand_obstructions()
# print(scene.obstruction_list)
scene.create_nodes()
print("Node List to iterate through:")
scene.node_list.print_list()
# scene.plot_nodes()
# scene.solve_initial_path()
scene.solve_puck_paths()
# scene.solve_return_path()

# plt.plot([1,2,3,4], [1,4,9,16], 'ro')
# plt.axis([0, 6, 0, 20])
# plt.show()





# block1.expand_bbox(end_effector_r)
# print (str(block1))	
# block2.expand_bbox(end_effector_r)
# print (str(block1))	
# # block3.expand_bbox(end_effector_r)
# print (str(block3))	

# print (str(block3.get_edges()))

# block1.get_lines()

# class NodeCreator:
	
# 	def __init__(self, block_list):
# 		self.block_list = block_list

# 	# def cell_decomp(self):
# 	# decomposes lines


# 	def solve(self):
# 		for i in block_list:
# 			for j in i.vertices
# 			# look at x co-ord and then see if any y vector woul




