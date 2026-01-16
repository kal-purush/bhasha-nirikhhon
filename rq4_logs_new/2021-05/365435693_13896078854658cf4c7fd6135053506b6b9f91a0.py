def initializeBoard():
    gameBoard = [['.', '.', '.'], ['.' ,'.' ,'.'], ['.', '.' ,'.']]
    return gameBoard


def printBoard(gameBoard):
    print("\n====== BOARD ======\n")
    print("      ", end="")
    for i in range(3):
        print("B=%d " % (i + 1), end="")
    print("\n")
    for i in range(3):
        print(" A=%d   " % (i + 1), end="")
        ithLine = gameBoard[i]
        for i in range(3):
            print(ithLine[i], "  ", end="")
        print("\n")
    print("===================\n")


def inputAB(currentPlayer):
    printBoard(gameBoard)
    print('a를 입력하세요{}'.format(currentPlayer))
    a= int(input())
    print('b를 입력하시요.')
    b=int(input())
    return a, b


def isRightRange(A, B):
    if A<=3 and B<=3 and A>=1 and B>=1:
        return True
    else:
        return False


def getABonBoard(A, B):
    A=A-1
    B=B-1
    return A,B


def isEmptyCell(gameBoard, A, B):
    if gameBoard[A][B]=='.':
        return True
    else :
        return False
def updateBoard(gameBoard, A, B, mark):
    gameBoard[A][B]=mark


def getNextPlayer(player):
    if player=='x':
       player='o'
       return player
    else :
        player='x'
        return player

def isBingoInA(gameBoard):
    if gameBoard[0][0]==gameBoard[0][1]==gameBoard[0][2]!='.':
        return True
    if gameBoard[1][0]==gameBoard[1][1]==gameBoard[1][2]!='.':
        return True
    if gameBoard[2][0]==gameBoard[2][1]==gameBoard[2][2]!='.':
        return True
    else :
        return False

def isBingoInB(gameBoard):
    if gameBoard[0][0]==gameBoard[1][0]==gameBoard[2][0]!='.':
        return True
    if gameBoard[0][1]==gameBoard[1][1]==gameBoard[2][1]!='.':
        return True
    if gameBoard[0][2]==gameBoard[1][2]==gameBoard[2][2]!='.':
        return True
    else :
        return False


def isBingoInDiagonal(gameBoard):
    if gameBoard[0][0]==gameBoard[1][1]==gameBoard[2][2]!='.':
        return True
    if gameBoard[0][2]==gameBoard[1][1]==gameBoard[2][2]!='.':
        return True
    else:
        return False


def isBingo(gameBoard):
    if isBingoInB(gameBoard):
        return True
    elif isBingoInDiagonal(gameBoard):
        return True
    elif isBingoInA(gameBoard):
        return True
    else :
        return False

def isFull(gameBoard):
    s=0
    u=0
    for s in range(2):
        for u in range(2):
            if gameBoard[s][u]=='.':
                return False
    return True




startPlayer = 'x'

isEndOfGame = False
gameBoard = initializeBoard()
player = startPlayer

while not isEndOfGame:
    printBoard(gameBoard)
    inputA, inputB = inputAB(player)

    if isRightRange(inputA, inputB):
        A, B = getABonBoard(inputA, inputB)

        if isEmptyCell(gameBoard, A, B):
            updateBoard(gameBoard, A, B, player)

            if isBingo(gameBoard):
                printBoard(gameBoard)
                print("축하합니다!!! 승자는: %s 입니다." % player)
                isEndOfGame = True
            elif isFull(gameBoard):
                printBoard(gameBoard)
                print("보드판이 다 찼습니다. 숫자를 다시 입력해주세요.")
                isEndOfGame = True
            else:
                player = getNextPlayer(player)

        else:
            print("이미 채워진 칸입니다. 숫자를 다시 입력해주세요.")

    else:
        print("범위를 벗어난 숫자를 선택하였습니다. 숫자를 다시 입력해주세요.")