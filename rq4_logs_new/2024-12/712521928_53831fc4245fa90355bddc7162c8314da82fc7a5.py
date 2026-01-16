"""
51. N-Queens

The n-queens puzzle is the problem of placing n queens on an nxn chessboard
such that no two queens attack each other.

Given an integer n, return all distinct solutions to the n-queens puzzle.
You may return the answer in any order.

Each solution contains a distinct board configuration of the n-queens'
placement, where 'Q' and '.' both indicate a queen and an empty space,
respectively.
"""

"""
Example 1:
Input: n = 4
Output: [[".Q..","...Q","Q...","..Q."],["..Q.","Q...","...Q",".Q.."]]

Explanation: There exist two distinct solutions to the 4-queens puzzle as shown above

Example 2:
Input: n = 1
Output: [["Q"]]
"""

class Solution:
    def solveNQueens(self, n: int) -> list[list[str]]:
        solutions = []
        
        board = [-1]*n #these are actually 4 columns
        
        def is_safe(row, col):
            for prev_row in range(row):
                prev_col = board[prev_row]
                # Column Check
                if prev_col == col:
                    return False
                # Diagonal Check
                if abs(prev_row - row) == abs(prev_col - col):
                    return False
            return True

        def backtracking(row):
            # Success Case
            if row == n:
                result = []
                for r in range(n):
                    row_str = ['.']*n
                    row_str[board[r]] = 'Q'
                    result.append("".join(row_str))
                solutions.append(result)
                return

            for col in range(n):
                if is_safe(row, col):
                    board[row] = col
                    backtracking(row + 1)

        backtracking(0)
        return solutions

class Solution:
    def solveNQueens(self, n: int) -> list[list[str]]:
        solutions = []
        # Initialize the board with -1 (no queen)
        board = [[-1]*n for _ in range(n)]

        def is_safe(row, col):
            # Check the column above current row
            for i in range(row):
                if board[i][col] == 1:
                    return False

            # Check upper-left diagonal
            i, j = row - 1, col - 1
            while i >= 0 and j >= 0:
                if board[i][j] == 1:
                    return False
                i -= 1
                j -= 1

            # Check upper-right diagonal
            i, j = row - 1, col + 1
            while i >= 0 and j < n:
                if board[i][j] == 1:
                    return False
                i -= 1
                j += 1

            return True

        def backtrack(row=0):
            # If we've placed all queens
            if row == n:
                # Convert the board to the required format
                solution = []
                for r in range(n):
                    row_str = []
                    for c in range(n):
                        row_str.append('Q' if board[r][c] == 1 else '.')
                    solution.append("".join(row_str))
                solutions.append(solution)
                return

            # Try placing a queen in each column of this row
            for col in range(n):
                if is_safe(row, col):
                    board[row][col] = 1
                    backtrack(row + 1)
                    board[row][col] = -1  # Backtrack: remove the queen

        backtrack(0)
        return solutions

sol = Solution()
print(sol.solveNQueens(n = 4))