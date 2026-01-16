def neighbours(i, j, m, n):
    x_diff = [0,0,1,-1]
    y_diff = [1,-1,0,0]
    nbrs = []

    for k in range(4):
        if i+x_diff[k]<m and i+x_diff[k]>=0 and j+y_diff[k]<n and j+y_diff[k]>=0:
            nbrs.append((i+x_diff[k], j+y_diff[k]))
    
    return nbrs

def count_islands(grid):
    
    c = 0
    m, n = len(grid), len(grid[0])
    visited = [[0]*n for i in range(m)]

    for i in range(m):
        for j in range(n):

            if grid[i][j]=='1' and visited[i][j]!=1: 
                c += 1
                S = [(i,j)]
                while S:
                    x,y = S.pop()
                    visited[x][y] = 1

                    for nbr in neighbours(x, y, m, n):
                        if grid[nbr[0]][nbr[1]]=='1' and visited[nbr[0]][nbr[1]]!=1:
                            visited[nbr[0]][nbr[1]] = 1
                            S.append((nbr[0],nbr[1]))
            else:
                continue
    
    return c

grid = [
  ["1","1","0","0","0"],
  ["1","1","0","0","0"],
  ["0","0","1","0","0"],
  ["0","0","0","1","1"]
]

count_islands(grid)

grid = ['1']
count_islands(grid)

grid = ['0']
count_islands(grid)

grid = ['1','1','0','1','1']
count_islands(grid)

