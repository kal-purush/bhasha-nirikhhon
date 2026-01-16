"""
https://leetcode.com/problems/count-servers-that-communicate/
1267. Count Servers that Communicate
You are given a map of a server center, represented as a m * n integer matrix grid, where 1 means that on that cell there 
is a server and 0 means that it is no server. Two servers are said to communicate if they are on the same row or on the same column.
Return the number of servers that communicate with any other server.
Example 1:
Input: grid = [[1,0],[0,1]]
Output: 0
Explanation: No servers can communicate with others.
Example 2:
Input: grid = [[1,0],[1,1]]
Output: 3
Explanation: All three servers can communicate with at least one other server.
Example 3:
Input: grid = [[1,1,0,0],[0,0,1,0],[0,0,1,0],[0,0,0,1]]
Output: 4
Explanation: The two servers in the first row can communicate with each other. The two servers in the third column can communicate with each other. The server at right bottom corner can't communicate with any other server.
Constraints:
m == grid.length
n == grid[i].length
1 <= m <= 250
1 <= n <= 250
grid[i][j] == 0 or 1
*** Note: Solution mode is pre-compute,  we walk once the grid to check how many servers there are in each row and col.
*** then we walk the grig again and check if [i][j] is = 1 and its col or row has more than 1 service this count as a connected
*** service and added to a count. The we made 2 m*n process, The complexity is O(n*m)
"""

def countServers(grid: list[list[int]]) -> int :
    rows = [0] * len(grid)
    cols = [0] * len(grid[0])
    for i in range(len(grid)) :
        for j in range(len(grid[0])) :
            if grid[i][j] == 1 :
                rows[i] += 1
                cols[j] += 1
    count = 0 
    for i in range(len(grid)) :
        for j in range(len(grid[0])) :
            if grid[i][j] == 1 and (rows[i] > 1 or cols[j] > 1) :
                count +=1
    return count


grid = [[1,1,0,0],[0,0,1,0],[0,0,1,0],[0,0,0,1]]
print(countServers(grid))