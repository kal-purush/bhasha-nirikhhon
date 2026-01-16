import sys

if len(sys.argv) != 2:
    print("Usage: python3 Alice.py <inputfilename>")
    sys.exit()

# Here is how you open a file whose name is given as the first argument
f = open(sys.argv[1])

# Read in size of grid.
grid_size = int(f.readline())

global graph
graph = []
for line in f:
    row_nodes = line.split()
    graph.append(row_nodes)

global nodes_in_next_layer
nodes_in_next_layer = 0


# Initialize a matrix to keep track of visited nodes with step sizes.




def solve_maze(inputfilename):
    nodes_left_in_layer = 1
    global nodes_in_next_layer
    move_count = 0
    curr_path = []
    global graph
    parents = {}
    reached_goal = False

    visited = []
    for i in range(0, grid_size):
        row = []
        for j in range(0, grid_size):
            row.append([])
        visited.append(row)

    step_size = 1
    start = (4, 2, step_size)
    curr_path.append(start)
    visited[start[0]][start[1]].append(step_size)
    while len(curr_path) > 0:
        r = curr_path.pop(0)
        if graph[r[0]][r[1]] == "G":

            reached_goal = True
            break

        get_destinations(r, r[2], graph, visited, curr_path, parents)
        nodes_left_in_layer = nodes_left_in_layer - 1
        if nodes_left_in_layer == 0:
            nodes_left_in_layer = nodes_in_next_layer
            nodes_in_next_layer = 0
            move_count = move_count + 1
    if reached_goal:
        print(move_count)

        final_path = []
        curr_node = (r[0],r[1], r[2])
        for i in range(0, move_count):
            final_path.insert(0, curr_node)
            # print(parents[curr_node])
            curr_node = parents[curr_node]

        final_path.insert(0, start)

        directional_path = []
        for i in range(0, move_count):
            if final_path[i][0] < final_path[i + 1][0] and final_path[i][1] == final_path[i + 1][1]:
                directional_path.append(("S", final_path[i + 1][2]))
            elif final_path[i][0] > final_path[i + 1][0] and final_path[i][1] == final_path[i + 1][1]:
                directional_path.append(("N", final_path[i + 1][2]))
            elif final_path[i][0] == final_path[i + 1][0] and final_path[i][1] < final_path[i + 1][1]:
                directional_path.append(("E", final_path[i + 1][2]))
            elif final_path[i][0] == final_path[i + 1][0] and final_path[i][1] > final_path[i + 1][1]:
                directional_path.append(("W", final_path[i + 1][2]))
            elif final_path[i][0] < final_path[i + 1][0] and final_path[i][1] < final_path[i + 1][1]:
                directional_path.append(("SE", final_path[i + 1][2]))
            elif final_path[i][0] < final_path[i + 1][0] and final_path[i][1] > final_path[i + 1][1]:
                directional_path.append(("SW", final_path[i + 1][2]))
            elif final_path[i][0] > final_path[i + 1][0] and final_path[i][1] < final_path[i + 1][1]:
                directional_path.append(("NE", final_path[i + 1][2]))
            elif final_path[i][0] > final_path[i + 1][0] and final_path[i][1] > final_path[i + 1][1]:
                directional_path.append(("NW", final_path[i + 1][2]))

        print(directional_path)

    return -1

def get_destinations(node, step_size, graph, visited, curr_path, parents):


    global nodes_in_next_layer

    directions = graph[node[0]][node[1]].split("-")

    if int(directions[0]) == 1:
        step_size = step_size + 1
    elif int(directions[0]) == 2:
        step_size = step_size - 1

    for direction in directions[1:]:

        if direction == "N":
            destination = (node[0] - step_size, node[1], step_size)
        elif direction == "S":
            destination = (node[0] + step_size, node[1], step_size)
        elif direction == "E":
            destination = (node[0], node[1] + step_size, step_size)
        elif direction == "W":
            destination = (node[0], node[1] - step_size, step_size)
        elif direction == "NW":
            destination = (node[0] - step_size, node[1] - step_size, step_size)
        elif direction == "NE":
            destination = (node[0] - step_size, node[1] + step_size, step_size)
        elif direction == "SW":
            destination = (node[0] + step_size, node[1] - step_size, step_size)
        elif direction == "SE":
            destination = (node[0] + step_size, node[1] + step_size, step_size)
        else:
            destination = (-1, -1, step_size)
        if destination[0] < 0 or destination[0] >= grid_size or destination[1] < 0 or destination[1] >= grid_size:
            continue

        if step_size in visited[destination[0]][destination[1]]:
            continue

        curr_path.append(destination)
        parents[destination] = node
        # print(visited)
        visited[destination[0]][destination[1]].append(step_size)
        nodes_in_next_layer = nodes_in_next_layer + 1


solve_maze(sys.argv[1])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    solve_maze(sys.argv[1])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/