# DESCRIPTION
# Given a 2d grid map of '1's (land) and '0's (water),
# count the number of islands. An island is surrounded by water
# and is formed by connecting adjacent lands horizontally or vertically.
# You may assume all four edges of the grid are all surrounded by water.

# EXAMPLE 1:
# Input:
# 11110
# 11010
# 11000
# 00000
# Output: 1

# EXAMPLE 2:
# Input:
# 11000
# 11000
# 00100
# 00011
# Output: 3

class Solution:
    def numIslands(self, grid: List[List[str]]) -> int:
        '''
        Time: O(N^2), we have to dfs each element in the grid
        Space: O(N), stack frames from dfs at worst point
        '''
        # check if grid exists or if the grid has length 0
        if not grid or len(grid) == 0:
            return 0

        # island counter
        num_islands = 0

        # must iterate over every element
        for i in range(len(grid)):
            for j in range(len(grid[i])):
                if grid[i][j] == '1':
                    # when an island appears increment the counter
                    num_islands += self.dfs(grid, i , j)

        return num_islands

    def dfs(self, grid, i , j):
        # row goes up
        # row goes too low
        # column goes too left
        # column goes too right
        # current element has been seen/ ran out of land
        # exit
        if i < 0 or i >= len(grid) or j < 0 or j >= len(grid[i]) or grid[i][j] == '0':
            return 0

        # backtracking: set the current element has seen
        grid[i][j] = '0'
        # search right
        self.dfs(grid, i, j + 1)
        # search left
        self.dfs(grid, i, j - 1)
        # search up
        self.dfs(grid, i + 1, j)
        # search down
        self.dfs(grid, i - 1, j)

        # count the element when it was sent from the main loop
        return 1