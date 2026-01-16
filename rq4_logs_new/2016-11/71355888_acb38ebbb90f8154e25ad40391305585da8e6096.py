import numpy.testing as npt
from board import ChessBoard
board1 = [
    [-1, -2, -3, -5, -4, -3, -2, -1],
    [-6, -6, -6, -6, -6, -6, -6, -6],
    [0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0],
    [6, 6, 6, 6, 6, 6, 6, 6],
    [1, 2, 3, 5, 4, 3, 2, 1]
]

board2 = [
    [0, 0, 0, 0, -5, 0, 0, -4],
    [0, 0, -1, 0, -1, 0, 0, 0],
    [0, 0, 0, 0, 6, 1, 0, -6],
    [-6, 0, -6, 0, 0, 0, 0, 0],
    [6, 0, 3, -6, 0, 5, 0, 6],
    [0, 6, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 6, 0],
    [0, 0, 0, 0, 0, 0, 4, 0]
]

board3 = [
    [-1, -2, -3, -5, -4, -3, 0, -1],
    [0, -6, 0, 0, -6, -6, -6, -6],
    [-6, 0, 0, -6, 0, -2, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 2, 6, 0, 0, 0],
    [0, 0, 2, 0, 0, 0, 0, 0],
    [6, 6, 6, 0, 3, 6, 6, 6],
    [1, 0, 3, 5, 4, 0, 0, 1]
]

board4 = [
    [0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 0, 1],
    [0, 0, 0, 0, 0, -5, 0, -4],
    [0, 0, 0, 5, 0, 0, 2, 0],
    [0, 0, 0, -6, 0, 0, 0, 0],
    [6, 6, 0, 0, 0, -6, 6, 6],
    [0, 0, 0, 0, 0, -2, 0, 4],
    [0, 0, 0, 0, -1, 0, 0, 0]
]

board5 = [
    [0, 0, 0, -5, -2, -1, -4, 0],
    [0, 0, 0, -1, -3, -6, -6, -6],
    [-6, -6, -2, -6, -6, 0, 0, 0],
    [0, 0, 0, 0, 0, 0, 2, 0],
    [0, 0, 6, 0, 2, 5, 0, 0],
    [0, 6, 0, 0, 0, 0, 6, 0],
    [6, 3, 0, 0, 6, 6, 4, 6],
    [0, 0, 1, 1, 0, 0, 0, 0]
]

print('test1')
board = ChessBoard('./images/tests/1.png')
npt.assert_equal(board1, board.get_board())

print('test2')
board = ChessBoard('./images/tests/2.png')
npt.assert_equal(board2, board.get_board())

print('test3')
board = ChessBoard('./images/tests/3.png')
npt.assert_equal(board3, board.get_board())

print('test4')
board = ChessBoard('./images/tests/4.png')
npt.assert_equal(board4, board.get_board())

print('test5')
board = ChessBoard('./images/tests/5.png')
npt.assert_equal(board5, board.get_board())