#As taught in class using BFS to solvee this problem
#Time Complexity: O(m*n)
#Space Complexity: O(m*n)
def hasPath(self, maze: List[List[int]], start: List[int], destination: List[int]) -> bool:
    if maze is None or len(maze)==0:
        return False
    q = list()
    m = len(maze)
    n = len(maze[0])
    q.append(start)
    maze[start[0]][start[1]] = 1
    dir = [(0,1),(0,-1),(1,0),(-1,0)]
    while (len(q)!=0):
        curr = q.pop(0)
        if curr[0] == destination[0] and curr[1] == destination[1]:
            return True
        for d in dir:
            i = curr[0]
            j = curr[1]
            while i>0 and j>=0 and i < m and j <n and maze[i][j]!=1:
                i = i + d[0]
                j = j + d[1]
            if maze[i][j]!=2:
                q.append((i,j))
                maze[i][j] = 2
    return False