"""
62. Unique Paths

There is a robot on an mxn grid. The robot is initially located at the top-left corner (i.e. grid[0][0]).
The robot tries to move to the bottom-right corner (i.e., grid[m-1][n-1]). The robot can only move
either down or right at any point in time.

Given the two integers m and n, return the number of possible unique paths that the robot can take to
reach the bottom-right corner.

The test cases are generated so that the answer will be less than or equal to 2*10^9.
"""

"""
Example 1:
Input: m = 3, n = 7
Output: 28

Example 2:
Input: m = 3, n = 2
Output: 3
Explanation: From the top-left corner, there are a total of 3 ways to reach the bottom-right corner:
1. Right -> Down -> Down
2. Down -> Down -> Right
3. Down -> Right -> Down
"""

class Solution:
    def uniquePaths(self, m: int, n: int) -> int:

        dp = [[0] * n for _ in range(m)]
        # Base Case #1
        dp[0][0] = 1
        # Base Case #2
        for col in range(n):
            dp[0][col] = 1
        # Base Case #3
        for row in range(m):
            dp[row][0] = 1

        # Start Filling Tables
        for row in range(1, m):
            for col in range(1, n):
                #If you are at any cell (i, j), you could only have come from the cell above (i-1, j) or the cell to the left (i, j-1).
                #Hence, the total paths to (i, j) is the sum of the paths to (i-1, j) and to (i, j-1).
                dp[row][col] = dp[row - 1][col] + dp[row][col - 1]

        return dp[m-1][n-1]

class Solution:
    def uniquePaths(self, m: int, n: int) -> int:

        dp = [[1] * n for _ in range(m)]

        # Start Filling Tables
        for row in range(1, m):
            for col in range(1, n):
                #If you are at any cell (i, j), you could only have come from the cell above (i-1, j) or the cell to the left (i, j-1).
                #Hence, the total paths to (i, j) is the sum of the paths to (i-1, j) and to (i, j-1).
                dp[row][col] = dp[row - 1][col] + dp[row][col - 1]

        return dp[m-1][n-1]

# Combinatorial Approach - Binomial Problem (Down or Right)
class Solution:
    def uniquePaths(self, m: int, n: int) -> int:
        import math

        total_no_of_possible_moves = (m-1) + (n-1)
        possible_down_moves = (m-1)
        possible_right_moves = (n-1)

        return math.factorial(total_no_of_possible_moves) // math.factorial(possible_down_moves) * math.factorial(possible_right_moves)

sol = Solution()
print(sol.uniquePaths(m=3, n=2))