# Chessboard Class and Main method

from graphics import *
from Square import *
from Button import *

class Chessboard:

    def __init__(self, win):

        self.win = win
        self.board = [[],[],[],[],[],[],[],[]]
        for i in range(8):
            for j in range(8):
                self.board[i].append(Square(Point(100 + 80*i, 100 + 80*j), win))

    def getClick(self, m):
        for i in range(8):
            for j in range(8):
                if self.board[i][j].isClicked(m):
                    return self.board[i][j]
        return False

    def getSquare(self, x, y):
        return self.board[x][y]

def main():

    win = GraphWin("Chess example", 1000, 800)

    Q = Button(win, Point(750, 680), Point(850, 760), "tomato", "QUIT")

    C = Chessboard(win)

    k1 = Piece(Point(140, 140), "knight", win)
    k2 = Piece(Point(300, 460), "knight", win)
    C.getSquare(0,0).addPiece(k1)
    C.getSquare(2,4).addPiece(k2)

    while True:
        m = win.getMouse()

        if Q.isClicked(m):
            break

        B = C.getClick(m)
        if B:
            print(B.getPos())
            if B.hasPiece():
                print(B.getPiece().getName())
                print(B.getPiece().getPos())

    win.close()

    

if __name__ == "__main__":
    main()