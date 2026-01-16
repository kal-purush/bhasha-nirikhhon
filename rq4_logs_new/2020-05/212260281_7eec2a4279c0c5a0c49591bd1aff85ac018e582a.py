# DESCRIPTION
# Given a m x n grid filled with non-negative numbers,
# find a path from top left to bottom right which
# minimizes the sum of all numbers along its path.
# Note: You can only move either down or right at any point in time.

# EXAMPLE:
# Input:
# [
#   [1,3,1],
#   [1,5,1],
#   [4,2,1]
# ]
# Output: 7
# Explanation: Because the path 1→3→1→1→1 minimizes the sum.

class Solution:
    def minPathSum(self, grid: List[List[int]]) -> int:
        '''
        Time: O(M*N), size of grid
        Space: O(M*N), space of the dp grid
        '''
        if not grid or len(grid) == 0:
            return 0

        N = len(grid)
        path_sum = [[0 for y in range(len(grid[0]))] for x in range(N)]

        for i in range(N):
            for j in range(len(grid[i])):
                # fill the array with the given grid
                path_sum[i][j] += grid[i][j]
                # when inside the meat of the array
                # not the first row and not first column
                if i > 0 and j > 0:
                    # choose the smaller value
                    path_sum[i][j] += min(path_sum[i-1][j], path_sum[i][j-1])
                # take the left element and add it to the sum array
                # only when the column is not on the edge
                # only the top row
                elif j > 0:
                    # add left
                    path_sum[i][j] += path_sum[i][j-1]
                # only the first column
                elif i > 0:
                    # add top
                    path_sum[i][j] += path_sum[i-1][j]

        # bottom right gives us the minimum path sum
        return path_sum[N-1][len(grid[0])-1]