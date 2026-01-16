def neighbors_greater(i, j, m, n, matrix):

    x_diff = [0, 0, 1, -1]
    y_diff = [1, -1, 0, 0]

    nbrs = []

    for k in range(4):
        x = x_diff[k]
        y = y_diff[k]
        if x>=0 and x<m and y>=0 and y<n and matrix[x][y]>matrix[i][j]:
            nbrs.append((x, y))
                
    return nbrs 

def longest_increasing_path(matrix):

    m, n = len(matrix), len(matrix[0])

    if m==0 and n==0:
        return 0

    if m==1 and n==1:
        return 1 

    opt = [[0]*n for i in range(m)]
    max_path = 0

    for i in range(m):
        for j in range(n):
            max_path = max(max_path, dfs_lis(i, j, m, n, matrix, opt))
    
    return max_path


def dfs_lis(i, j, m, n, M, opt):

    if opt[j][j]!=0:
        return opt[i][j]

    nbrs = neighbors_greater(i, j, m, n, M)

    opt[i][j] = 0

    for nbr in nbrs:
        opt[i][j] = max(opt[i][j], dfs_lis(nbr[0], nbr[1], m, n, M, opt))

    opt[i][j] += 1

    return opt[i][j] 

matrix = [[9,9,4],[6,6,8],[2,1,1]]
longest_increasing_path(matrix)

matrix = [[3,4,5],[3,2,6],[2,2,1]]
longest_increasing_path(matrix)



