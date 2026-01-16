
ski_map = """4 4 
4 8 7 3
2 6 9 3
4 5 4 2
2 3 1 6"""

def parse_input(input_str):
	lines = input_str.split("\n")[1:]
	return [map(lambda x: int(x), line.split(" ")) for line in lines]

def read_file(file_name):
	print "Loading map"
	ski_map_file = open(file_name, "r")
	raw_ski_map = ski_map_file.readlines()
	ski_map_file.close()
	ski_map = [map(lambda x: int(x), line.split(" ")) for line in raw_ski_map[1:]]
	return ski_map


def update_algorithm(ski_map):
	print "Performing update algorithm\n---------------------"
	longest_path_map = [[0 for i in range(len(input_line))] for input_line in ski_map]
	ending_point_map = [[10000 for i in range(len(input_line))] for input_line in ski_map]
	neighbour_queue = {}
	# init for points with path length 1
	print "Scanning neighbours"
	for i in range(len(longest_path_map)):
		for j in range(len(longest_path_map[i])):
			if scan_neighbours(ski_map, i, j):
				longest_path_map[i][j] = 1
				push_neighbours(ski_map, neighbour_queue, i, j)
				ending_point_map[i][j] = ski_map[i][j]

	print "Performing algorithm iterations"
	modified = True
	iteration_count = 0
	while modified:
		old_modified = modified
		new_neighbour_queue = {}
		modified = []
		for original in neighbour_queue:
			for neighbour in neighbour_queue[original]:
				i,j = original
				new_i, new_j = i+neighbour[0], j+neighbour[1]
				if ski_map[i][j] < ski_map[new_i][new_j]:
					longest_path_map[new_i][new_j] = longest_path_map[i][j] + 1
					modified.append((new_i, new_j))
					if (new_i,new_j) in new_neighbour_queue:
						ending_point_map[new_i][new_j] = min(ending_point_map[new_i][new_j], ending_point_map[i][j])
					else:
						ending_point_map[new_i][new_j] = ending_point_map[i][j]
						push_neighbours(ski_map, new_neighbour_queue, new_i, new_j)
					# if ending_point_map[new_i][new_j] > ending_point_map[i][j]:
						# ending_point_map[new_i][new_j] = ending_point_map[i][j]

		iteration_count += 1
		if new_neighbour_queue == neighbour_queue:
			raise AssertionError("endless loop die die die")
		neighbour_queue = new_neighbour_queue
		print iteration_count

	print "Starting points found: " + str(old_modified)
	print "Maximum height: " + str(longest_path_map[old_modified[0][0]][old_modified[0][1]]) 
	print "Starting heights: " + str([ski_map[coord[0]][coord[1]] for coord in old_modified])
	print "Ending heights: " + str([ending_point_map[coord[0]][coord[1]] for coord in old_modified])
	print "Maximum drop: " + str(max([ski_map[i][j] - ending_point_map[i][j] for i,j in old_modified]))

	answer_map = open("ending_update_algo.txt", "w")
	for path in ending_point_map:
		answer_map.write(" ".join([str(square) for square in path]))
		answer_map.write("\n")
	answer_map.close()

def max_algorithm(ski_map):
	print "Performing max DP algorithm\n---------------------"
	longest_path_map = [[0 for i in range(len(input_line))] for input_line in ski_map]
	ending_point_map = [[10000 for i in range(len(input_line))] for input_line in ski_map]
	max_path = 0
	starting_points = []
	for i in range(len(ski_map)):
		for j in range(len(ski_map[0])):
			if not longest_path_map[i][j]:
				find_max(ski_map, longest_path_map,ending_point_map, i, j)
			if longest_path_map[i][j] > max_path:
				max_path = longest_path_map[i][j]
				starting_points = [(i,j)]
			elif longest_path_map[i][j] == max_path:
				starting_points.append((i,j))
	answer_map = open("ending_max_algo.txt", "w")
	for path in ending_point_map:
		answer_map.write(" ".join([str(square) for square in path]))
		answer_map.write("\n")
	answer_map.close()
	print "Starting points found: " + str(starting_points)
	print "Maximum height: " + str(max_path)
	print "Starting heights: " + str([ski_map[i][j] for i,j in starting_points])
	print "Ending heights: " + str([ending_point_map[i][j] for i,j in starting_points])
	print "Maximum drop: " + str(max([ski_map[i][j] - ending_point_map[i][j] for i,j in starting_points]))

def find_max(ski_map, longest_path_map, ending_point_map, i, j):
	current_height = ski_map[i][j]
	max_path = 0
	ending_point_map[i][j] = ski_map[i][j]
	for neighbour in get_neighbours(ski_map, i, j):
		new_i, new_j = neighbour[0] + i, neighbour[1] + j
		if ski_map[new_i][new_j] < current_height:
			if longest_path_map[new_i][new_j] == 0:
				find_max(ski_map, longest_path_map, ending_point_map, new_i, new_j)
			if longest_path_map[new_i][new_j] > max_path:
				max_path = longest_path_map[new_i][new_j]
				ending_point_map[i][j] = ending_point_map[new_i][new_j]
			elif longest_path_map[new_i][new_j] == max_path and ending_point_map[i][j] > ending_point_map[new_i][new_j]:
				ending_point_map[i][j] = ending_point_map[new_i][new_j]
	longest_path_map[i][j] = max_path + 1

def push_neighbours(ski_map, work_queue, i, j):
	work_queue[i,j] = []
	for neighbour in get_neighbours(ski_map, i, j):
		work_queue[i,j].append((neighbour[0], neighbour[1]))

def scan_neighbours(ski_map, i, j):
	for neighbour in get_neighbours(ski_map, i, j):
		if ski_map[i+neighbour[0]][j+neighbour[1]] < ski_map[i][j]:
			return False
	return True

def get_neighbours(ski_map, i, j):
	neighbours = []
	if i > 0:
		neighbours.append((-1,0))
	if j > 0: 
		neighbours.append((0,-1))
	if i < len(ski_map) - 1: 
		neighbours.append((1,0))
	if j < len(ski_map[0]) - 1: 
		neighbours.append((0,1))
	return neighbours


# update_algorithm(parse_input(ski_map))
# max_algorithm(parse_input(ski_map))
max_algorithm(read_file("ski_map.txt"))
# update_algorithm(read_file("ski_map.txt"))