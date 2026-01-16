from state import find_all_neighbors, find_neighbors, find_wall_neighbors, find_neighbors_dfs, find_neighbors_belief
from custom_maze import maze

def beliefstate(maze, goal_pos):
    # we only know where the goal is
    # we do not know where we are going to start
    # we need to know where we need to go at any starting position
    
    # let's try starting from the goal position and populate all free spaces and mark their distance from the goal
    n = len(maze)
    m = len(maze[0])
    distance_map = [[-1 for _ in range(m)] for _ in range(n)]
    def dfs(curr_pos, distance):
        nonlocal goal_pos
        nonlocal visited
        distance_map[curr_pos[0]][curr_pos[1]] = distance
        visited.add(curr_pos)
        neighbours = find_neighbors_belief(maze, curr_pos)
        for neighbour in neighbours:
            if neighbour not in visited:
                # new_path = curr_path[:]
                # new_path.append(neighbour)
                dfs(neighbour, distance + 1)
    visited = set()
    # valid_paths = []
    dfs(goal_pos, 0)
    return distance_map
print(beliefstate(maze, (0,0)))
