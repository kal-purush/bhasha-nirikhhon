import sys

def get_start_position(maze, n, m):

    x0 = int(input("Which row should our robot be on?\n"))
    if x0 < 0 or x0 >= n:
        print("Invalid row number: should be between 0 and n\n")
        get_start_position(maze, n, m)
        return
    
    y0 = int(input("Which column should our robot be on?\n"))
    if y0 < 0 or y0 >= m:
        print("Invalid column number: should be between 0 and m\n")
        get_start_position(maze, n, m)
        return
    
    if maze[x0][y0]==1:
        print("Invalid position: you can not start on a wall cell\n")
        get_start_position(maze, n, m)
        return

    return (x0,y0)

def get_goal_position(maze, n, m):

    x1 = int(input("Which row should our robot get to?"))
    if x1 < 0 or x1 >= n:
        print("Invalid row number: should be between 0 and n")
        get_goal_position(maze, n, m)
        return
    y1 = int(input("which column should our robot get to?"))
    if y0 < 0 or y0 >= m:
        print("Invalid column number: should be between 0 and m")
        get_goal_position(maze, n, m)
        return
    
    if maze[x1][y1]==1:
        print("Invalid position: you can not end on a wall cell\n")
        get_goal_position(maze, n, m)
        return

    return (x1,y1)