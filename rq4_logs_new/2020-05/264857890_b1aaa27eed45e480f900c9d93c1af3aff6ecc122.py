# -------------------------------------------------------------------------------
# TacTical
# This program is designed to play Tac-Tical, using lookahead and board
# heuristics. It will allow the user to play a game against the machine, or
# allow the machine to play against itself for purposes of learning to improve
# its play.  All 'learning' code has been removed from this program.
#
# Tac-Tical is a 2-player game played on a grid.  Each player has the same
# number of tokens distributed on the grid in an initial configuration.  On each
# turn, a player may move one of his/her tokens one unit either horizontally or
# vertically (not diagonally) into an unoccupied square.  The object is to be
# the first player to get three tokens in a row, either horizontally,
# vertically, or diagonally.
#
# The board is represented by a matrix with extra rows and columns forming a
# boundary to the playing grid.  Squares in the playing grid can be occupied by
# either 'X', 'O', or 'EMPTY' spaces.  The extra elements are filled with 'Out
# of Bounds' squares, which makes some of the computations simpler.
# -------------------------------------------------------------------------------

from __future__ import print_function
import random
from random import randrange
import copy


def getMoves(player, board):
    # -------------------------------------------------------------------------
    # Determines all legal moves for player with current board,
    # and returns them in moveList.
    #
    # A move is represented by a list of 4 elements, representing 2 pairs of
    # coordinates, (fromRow, fromCol) and (toRow, toCol), which represent the
    # positions of the piece to be moved, before and after the move.
    # -------------------------------------------------------------------------

    moveList = []
    for i in range(1, NUM_ROWS + 1):
        for j in range(1, NUM_COLS + 1):
            if board[i][j] == player:
                # -------------------------------------------------------------
                #  Check move directions (m,n) = (-1,0), (0,-1), (0,1), (1,0)
                # -------------------------------------------------------------
                for m in range(-1, 2):
                    for n in range(-1, 2):
                        if abs(m) != abs(n):
                            if board[i + m][j + n] == EMPTY:
                                moveList.append([i, j, i + m, j + n])

    return moveList


def getHumanMove(player, board):
    # -------------------------------------------------------------------------
    # If the opponent is a human, the user is prompted to input a legal move.
    # Determine the set of all legal moves, then check input move against it.
    # -------------------------------------------------------------------------
    moveList = getMoves(player, board)
    move = None

    validMove = False
    while (not validMove):
        fromRow, fromCol, toRow, toCol = map(int, input('Input your move (fromRow, fromCol, toRow, toCol): ').split(' '))

        for m in moveList:
            if m == [fromRow, fromCol, toRow, toCol]:
                validMove = True
                move = m

        if validMove:
            return move

        print('Invalid move.  ')


def applyMove(board, move):
    # -------------------------------------------------------------------------
    # Perform the given move, and update board.
    # -------------------------------------------------------------------------

    fromRow, fromCol, toRow, toCol = move
    newBoard = copy.deepcopy(board)
    newBoard[toRow][toCol] = newBoard[fromRow][fromCol]
    newBoard[fromRow][fromCol] = EMPTY
    return newBoard


def initBoard():
    # -------------------------------------------------------------------------
    # Initialize the game board.
    # -------------------------------------------------------------------------

    for i in range(0, BOARD_ROWS + 1):
        for j in range(0, BOARD_COLS + 1):
            board[i][j] = OUT_OF_BOUNDS

    for i in range(1, NUM_ROWS + 1):
        for j in range(1, NUM_COLS + 1):
            board[i][j] = EMPTY

    for j in range(1, NUM_COLS + 1):
        if odd(j):
            board[1][j] = X_TOKEN
            board[NUM_ROWS][j] = O_TOKEN
        else:
            board[1][j] = O_TOKEN
            board[NUM_ROWS][j] = X_TOKEN

    return board


def odd(n):
    # -------------------------------------------------------------------------
    # Returns True if n is an odd integer.
    # -------------------------------------------------------------------------
    return n % 2 == 1


def showBoard(board):
    # -------------------------------------------------------------------------
    # Displays the board state.
    # -------------------------------------------------------------------------
    outstr = "\n     1   2   3   4\n"
    row_divider = "   +" + "-" * (NUM_COLS * 4 - 1) + "+"
    outstr += row_divider + "\n"

    for i in range(1, NUM_ROWS + 1):
        outstr += " {0} ".format(i)
        for j in range(1, NUM_COLS + 1):
            if board[i][j] == X_TOKEN:
                outstr += '| X '
            elif board[i][j] == O_TOKEN:
                outstr += '| O '
            elif board[i][j] == EMPTY:
                outstr += '|   '
        outstr += '|\n'
        outstr += row_divider + "\n"

    return outstr


def win(player, board):
    # ---------------------------------------------------------------------------
    # Determines if player has won, by finding '3 in a row'.
    # *** Student code needed here. ***
    # ---------------------------------------------------------------------------


    #There are 3 conditions that need to be checked for the win:
    # 1) vertical 3x consecutive
    # 2) Horizontal 3x consecutive
    # 3) Diagonal 3x consecutive

    boardRows = len(board)
    boardCols = len(board[0])

    # debugging code
    print("--------------")
    print("board rows:" + str(boardRows))
    print("board cols:" + str(boardCols))
    print("--------------")

    # looping for only the playable board
    count=0
    for i in range(1, boardRows-1):
        for j in range(1, boardCols-1):
            print("Checking------Row, Col: " + str(i) + ", " + str(j))
            count+=1
            point = (i, j)
            if(checkHorizontal(board,point) or checkVertical(board, point) or checkDiagonal(board,point)):
                return True

    print("Total positions checked: "+str(count))
    return False;

def checkHorizontal(board,point):
    limit = len(board)-4
    x=point[0]
    y=point[1]

    if(x<=limit):           # used limit to prevent outOfBoundsException;        in this grid there are 10 such points
        # print("Checking horz at, " + str(x) + ", " + str(y))
        if(board[x][y]==board[x+1][y]==board[x+2][y]!=0):          # if three horizontal positions are same return true
            return True

    return False

def checkVertical(board,point):
    limit = len(board[0]) - 4
    x = point[0]
    y = point[1]

    if(y<=limit):       # used limit to prevent outOfBoundsException;       in this grid there are 12 such points
        # print("Checking vert at, " + str(x) + ", " + str(y))
        if(board[x][y]==board[x][y+1]==board[x][y+2]!=0):           # if three vertical positions are same return true
            return True

    return False

def checkDiagonal(board,point):
    limit = 3  # can't be helped; this needs to be hardcoded per game rules; 3 here is the pair count (kinda)
    x = point[0]
    y = point[1]
    print("Checking diag at, " + str(x) + ", " + str(y))

    # there are 4 directions that can be checked: ++, +-, -+, --;       we need to determine applicable direction(s) for each point
    # In this instance, 16 points have ONE direction check, 4 points allow DOUBLE direction checks (4th row points)
    # all 20 points for this diagonal check

    pp = False              # check right-down dir?
    pm = False              # check right-up dir?
    mp = False              # check left-down dir?
    mm = False              # check left-up dir?

    # we find these^ by calculating the space available on up/down/left/right of each point (including the point)
    hPlus=(((len(board[0]))-2)-y) + 1           #could be made more generic by using board dimentions       4-x+1
    hMinus=y
    vPlus=((len(board)-2)-x) + 1                #could be made more generic by using board dimentions       5-y+1
    vMinus=x

    if(hPlus>=limit and vPlus>=limit):
        pp=True
        print('pp true')
    if(hPlus>=limit and vMinus>=limit):
        pm = True
        print('pm true')
    if(hMinus>=limit and vPlus>=limit):
        mp=True
        print('mp true')
    if(hMinus>=limit and vMinus>=limit):
        mm=True
        print('mm true')

    # now starts the checks
    if(pp):
        if(board[x][y] == board[x + 1][y + 1] == board[x + 2][y + 2] != 0):  # if three vertical positions are same return true
            return True

    if(pm):
        if (board[x][y] == board[x + 1][y - 1] == board[x + 2][y - 2] != 0):  # if three vertical positions are same return true
            return True

    if(mp):
        if (board[x][y] == board[x - 1][y + 1] == board[x - 2][y + 2] != 0):  # if three vertical positions are same return true
            return True

    if(mm):
        if (board[x][y] == board[x - 1][y - 1] == board[x - 2][y - 2] != 0):  # if three vertical positions are same return true
            return True

    # man that took a while; should be fail proof and generic though!
    return False

def h(player, board):
    # ---------------------------------------------------------------------------
    # Heuristic evaluation of board, presuming it is player's move.
    # *** Student code needed here. ***
    # Heuristic should not do further lookahead by calling miniMax.
    # This function estimates the value of the board at a terminal node.
    # ---------------------------------------------------------------------------

    return -1;


def miniMax(player, board, opponent, min, depth, MAX_DEPTH):
    # ---------------------------------------------------------------------------
    # Use MiniMax algorithm to determine best move for player to make for given
    # board.  Return the chosen move and the value of applying the heuristic to
    # the board.
    # To examine each of player's moves and evaluate them with no lookahead,
    # MAX_DEPTH should be set to 1.  To examine each of the opponent's moves,
    #  set MAX_DEPTH=2, etc.
    # Increase depth by 1 on each recursive call to miniMax.
    # min is the minimum value seen thus far by
    #
    # If a win is detected, the value returned should be INFINITY-depth.
    # This rates 'one move wins' higher than 'two move wins,' etc.  This ensures
    # that player moves toward a win, rather than simply toward the assurance of
    # a win.
    #
    # *** Student code needed here. ***
    # Alpha-Beta pruning is recommended for Extra Credit.
    # Argument list for this function may be altered as needed.
    # ---------------------------------------------------------------------------

    # This code just picks a random move, and needs to be replaced.
    moveList = getMoves(player, board)  # find all legal moves
    k = randrange(0, len(moveList))  # pick one at random
    move = moveList[k]
    value = h(player, board)
    return move, value  # return move and backed-up value


def getComputerMove(player, board):
    # ---------------------------------------------------------------------------
    # If the opponent is a computer, use artificial intelligence to select
    # the best move.
    # For this demo, a move is chosen at random from the list of legal moves.
    # ---------------------------------------------------------------------------
    opponent = X_TOKEN if player == O_TOKEN else O_TOKEN
    move, value = miniMax(player, board, opponent, INFINITY, 0, 0)
    return move


def playerMove(board, player, playerType):
    # ---------------------------------------------------------------------------
    # Depending on the player type, return either a Human move or Computer move.
    # ---------------------------------------------------------------------------
    if playerType == "Human":
        return getHumanMove(player, board)
    else:
        return getComputerMove(player, board)


def showInstructions():
    # ---------------------------------------------------------------------------
    # Brief instructions and display of initial board configuration.
    # ---------------------------------------------------------------------------
    print(showBoard(board))
    print(
        """
    The squares of the board are numbered by row and column, with '1 1'
    in the upper left corner, '1 2' directly to the right of '1 1', etc.
    
    Moves are of the form 'i j m n', where (i,j) is a square occupied
    by your piece, and (m,n) is the square to which you move it.
    You move the 'X' pieces.
    """)
    print("-" * 24)


if __name__ == "__main__":

    # -------------------------------------------------------------------------
    # Global constants:
    # -------------------------------------------------------------------------
    # board dimensions:
    # -------------------------------------------------------------------------
    NUM_ROWS = 5
    BOARD_ROWS = NUM_ROWS + 1

    NUM_COLS = 4
    BOARD_COLS = NUM_COLS + 1

    MAX_MOVES = 4 * NUM_COLS

    # -------------------------------------------------------------------------
    # values stored on the board:
    # -------------------------------------------------------------------------
    X_TOKEN = -1
    O_TOKEN = 1
    EMPTY = 0
    OUT_OF_BOUNDS = 2

    # -------------------------------------------------------------------------
    # operational constants:
    # -------------------------------------------------------------------------
    INFINITY = 10000  # Value of a winning board
    MAX_DEPTH = 4  # Maximum depth of recursion by miniMax

    # -------------------------------------------------------------------------
    # board is the grid on which the game is played:
    # -------------------------------------------------------------------------
    board = [[0 for col in range(BOARD_COLS + 1)] for row in range(BOARD_ROWS + 1)]
    board = initBoard()

    print("\n" + "-" * 24 + "\nWelcome to Tac-Tical!\n" + "-" * 24)
    ch = input("Do you want to see instructions (y/n)? ")
    if ch == 'y' or ch == 'Y':
        showInstructions()

    # -----------------------------------------------------------------------
    # Get player information
    # -----------------------------------------------------------------------
    playerName = [None for i in range(2)]
    playerToken = [None for i in range(2)]  # X or O
    playerType = [None for i in range(2)]  # "Human" or "Computer"

    print("Player 1 plays first.")
    for i in range(2):
        playerName[i] = input("\nName of Player " + str(i + 1) + ": ")
        ch = input("Human or Computer Player (h/c)? ")
        playerType[i] = "Human" if (ch == 'h' or ch == 'H') else "Computer"

    ch = input("Will " + playerName[0] + " play X or O (x/o)? ")
    playerToken[0] = X_TOKEN if (ch == 'x' or ch == 'X') else O_TOKEN
    playerToken[1] = O_TOKEN if (ch == 'x' or ch == 'X') else X_TOKEN

    # -----------------------------------------------------------------------
    # Testing Code
    # -----------------------------------------------------------------------
    print(showBoard(board))

    moveList = getMoves(X_TOKEN, board)
    print(moveList)

    moveList = getMoves(O_TOKEN, board)
    print(moveList)

    for n in range(5):
        move = playerMove(board, playerToken[0], playerType[0])
        board = applyMove(board, move)
        print(showBoard(board))


        win(playerToken[0],board)
        move = playerMove(board, playerToken[1], playerType[1])
        board = applyMove(board, move)
        print(showBoard(board))
        win(playerToken[0], board)