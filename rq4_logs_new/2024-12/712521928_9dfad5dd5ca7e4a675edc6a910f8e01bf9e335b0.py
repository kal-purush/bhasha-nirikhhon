"""
52. N-Queens II

The n-queens puzzle is the problem of placing n queens on an nxn chessboard
such that no two queens attack each other.

Given an integer n, return the number of distinct solutions to the
n-queens puzzle.
"""

"""
Example 1:
Input: n = 4
Output: 2

Explanation: There are two distinct solutions to the 4-queens puzzle as shown.

Example 2:
Input: n = 1
Output: 1
"""

class Solution:
    def totalNQueens(self, n: int) -> int:
        cols = set()
        diag1 = set() #row - col
        diag2 = set() #row + col
        
        def backtrack(row):
            if row == n:
                return 1
            
            solutions_count = 0
            for col in range(n):
                if col in cols or (row-col) in diag1 or (row+col) in diag2:
                    continue
                
                #placing queen
                cols.add(col)
                diag1.add(row-col)
                diag2.add(row+col)
                
                #move to next row
                solutions_count += backtrack(row+1)
                
                #backtrack - remove queen
                cols.remove(col)
                diag1.remove(row-col)
                diag2.remove(row+col)

            return solutions_count

        return backtrack(0)