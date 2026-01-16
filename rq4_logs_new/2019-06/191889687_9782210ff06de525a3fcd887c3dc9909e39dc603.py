from sys import  stdin
print "Welcome to the game"

board=[[0 for i in range(3)] for j in range(3)]


def newgame():
    for i in range(3):
        for j in range(3):
            board[i][j]=0
def printUI():
    for i in range(3):
        for j in range(3):
            if board[i][j]==0:
                print ".",
            else:
                print board[i][j],
        print '\n'
def isValid(coordinates):
    i,j=coordinates
    if i<3 and i>=0 and j<3 and j>=0:
        if board[i][j]==0:
            return True
    return False
def makemove(coordinates,player):
    i,j=coordinates
    board[i][j]=player

def checkrowwin(coordinates):
    i,j=coordinates
    if board[i][0]==board[i][1]==board[i][2]:
        return True
    return False

def checkcolwin(coordinates):
    i,j=coordinates
    if board[0][j] == board[1][j] == board[2][j]:
        return True
    return False

def checkdiagonalwin(coordinates):
    i,j=coordinates
    if i==j:
        if i==0:
            return board[i+1][j+1]==board[i+2][j+2]==board[i][j]
        elif i==1:
            return board[i+1][j+1]==board[i-1][j-1]==board[i][j]
        else:
            return board[i-1][j-1]==board[i-2][j-2]==board[i][j]
    else:
        return False
def check_win(coordinates):
    if checkrowwin(coordinates) or checkcolwin(coordinates) or checkdiagonalwin(coordinates):
        return True
    return False

if __name__=='__main__':
    newgame()
    player1=1
    player2=2
    select_player=0
    moves=0
    while True:
        printUI()
        print "Enter Coordinates:"
        coordinates=map(int,stdin.readline().split())
        if isValid(coordinates):
            if moves<9:
                if select_player==0:
                    makemove(coordinates,player1)
                    moves+=1
                    select_player=1-select_player
                    if check_win(coordinates):
                        print "Player 1 wins !"
                        break
                else:
                    makemove(coordinates, player2)
                    moves+=1
                    select_player = 1 - select_player
                    if check_win(coordinates):
                        print "Player 2 wins !"
                        break
            else:
                print 'Tie !'
                break
        else:
            print "Enter valid coordinates"

