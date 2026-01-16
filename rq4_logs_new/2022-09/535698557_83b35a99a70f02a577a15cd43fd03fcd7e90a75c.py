from custom_maze import maze
from state import find_neighbors, find_neighbors_dfs
from user_input import get_start_position

GOAL = 3
START = 2


  #Start Position
#robot_pos

def find_new_neighbors(maze, position, visited):
    n = len(maze)
    m = len(maze[0])
    x0,y0 = position
    neighbors = []
    for x1,y1 in [(x0-1,y0),(x0+1,y0),(x0,y0-1),(x0,y0+1)]:
        if x1>=0 and x1<n and y1>=0 and y1<m and maze[x1][y1]==0 and maze[x1][y1] not in visited :
            neighbors.append((x1,y1))
    #prev_robot = robot_pos[x0,y0]
    return neighbors

def recursive_backtracking(maze, start_pos, goal_pos): 
    def dfs(curr_pos, curr_path):
        nonlocal goal_pos
        nonlocal visited
        nonlocal valid_paths
        if curr_pos == goal_pos:
            #print("********")
            curr_path.append(curr_pos)
            valid_paths = curr_path
            return
        #print(curr_pos)
        visited.add(curr_pos)
        neighbours = find_neighbors_dfs(maze, curr_pos)
        for neighbour in neighbours:
            if neighbour not in visited:
                new_path = curr_path[:]
                new_path.append(neighbour)
                dfs(neighbour, new_path)
        #take the current_pos
    visited = set()
    valid_paths = None
    dfs(start_pos, [])
    return valid_paths
    
    '''
    visited = {}
    n= len(maze)
    m= len(maze[0])
    prev_robot = get_start_position(maze,n,m)  
    while robot_pos[x][y] != 3:
        new_nbors = find_new_neighbours(maze, current_pos, visited)
        if len(new_nbors) == 0:
            robot_pos = prev_robot
        elif robot_pos[x][y] == 3:
            break
        else:
            x,y = new_nbors[0]
            prev_robot = current_pos
            visited.add(prev_robot)
            robot_pos = robot_pos[x][y]
    return robot_pos
    '''

print(recursive_backtracking(maze, (3,4),(0,0)))
        
            
        