from state import find_all_neighbors, find_neighbors, find_wall_neighbors
from custom_maze import maze

def wavefront(maze, start_pos, goal_pos):
    n = len(maze)
    m = len(maze[0])
    virtual_maze = [[0 for _ in range(m)] for _ in range(n)]
    for i in range(n):
        for j in range(m):
            if maze[i][j] == 1:
                virtual_maze[i][j] = float("inf")
            elif maze[i][j] == 2:
                virtual_maze[i][j] == 2
            elif maze[i][j] == 3:
                virtual_maze[i][j] == 3
    
    # start at the goal position
    level = [goal_pos]
    number = 1
    visited = set()
    while len(level)!=0:
        new_level = []
        for pos in level:
            if pos not in visited:
                visited.add(pos)
                virtual_maze[pos[0]][pos[1]] = number
                neighbors = find_neighbors(maze, pos)
                new_level = new_level + neighbors
        level = new_level
        number += 1
    
    print(virtual_maze)
    curr_position = start_pos
    path = []
    while curr_position and curr_position != goal_pos:
        neighbors = find_neighbors(maze, curr_position)
        min_neighbor = None
        min_value = float("inf")
        for neighbor in neighbors:
            if virtual_maze[neighbor[0]][neighbor[1]] < min_value:
                min_neighbor = neighbor
                min_value = virtual_maze[neighbor[0]][neighbor[1]]
        virtual_maze[curr_position[0]][curr_position[1]] = float("inf")
        curr_position = min_neighbor
        path.append(curr_position)

    return path
    