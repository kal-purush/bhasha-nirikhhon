from custom_maze import maze
from state import find_neighbors

#import maze array
init_maze=maze
nbors=find_neighbors(init_maze,robo_start)

for i in len(init_maze):
    for j in len(init_maze[0]):
        if init_maze[i][j] == 0:
            init_maze[i][j] == 'free'
        elif init_maze[i][j]== 1
            init_maze[i][j]== 'wall'
        elif init_maze[i][j]==2
            init_maze[i][j]== 'start'
        elif init_maze [i][j]== 3
            init_maze[i][j] =='end'
            

fin_path=path_solver(init_maze)


def path_solver(init_maze):
    maze_start=list(zip(*np.where(init_maze=='end')))
    
    nbors=find_neighbors(init_maze,,robo_position)



