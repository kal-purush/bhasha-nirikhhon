#!/usr/bin/python3
""" Task N queens  """

import sys


def printResults(board):
    """ Print the answer """
    result = []
    for i in range(len(board)):
        for j in range(len(board)):
            if board[i][j] == 1:
                result.append([i, j])
    print(result)


def isSafe(board, row, col, n):
    """ Checking if a queen can be placed on board """
    for i in range(col):
        if board[row][i] == 1:
            return False

    for j, i in zip(range(row, -1, -1),
                    range(col, -1, -1)):
        if board[j][i] == 1:
            return False

    for j, i in zip(range(row, n, 1),
                    range(col, -1, -1)):
        if board[j][i] == 1:
            return False

    return True


def result(board, col, n):
    """ Result """
    if col == n:
        printResults(board)
        return True
    column = False
    for i in range(n):
        if isSafe(board, i, col, n):
            board[i][col] = 1
            column = result(board, col + 1, n) or column
            board[i][col] = 0
    return column


if __name__ == "__main__":
    """ Main """
    if not len(sys.argv) == 2:
        print("Usage: nqueens N")
        sys.exit(1)
    if not (sys.argv[1]).isdigit():
        print("N must be a number")
        sys.exit(1)
    n = int(sys.argv[1])
    if n < 4:
        print("N must be at least 4")
        sys.exit(1)

    board = [[0 for i in range(n)] for j in range(n)]
    result(board, 0, n)