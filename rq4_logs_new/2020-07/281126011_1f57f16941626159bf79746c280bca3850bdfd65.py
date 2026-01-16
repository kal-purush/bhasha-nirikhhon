"""
author:  MJ
date: 2020-07-27 -
"""


class Game:

    def __init__(self):
        self.positions = self.generateBoard()
        self.standing = []
        self.currentPiece = 1
        self.setPositions()
        self.nrOfMoves = 0
        self.gameOver = False

    # -----------------------------------------------------#

    'Reset board'
    def resetBoard(self):
        self.positions = self.generateBoard()
        self.standing = []
        self.setPositions()
        self.nrOfMoves = 0
        self.gameOver = False

    'Generate the board.'

    def generateBoard(self):
        positions = []
        nrOfPositions = 8 * 8
        first = 0  # Since 0%8 == 0, below
        second = 0

        for i in range(nrOfPositions):  # for every position on the board,
            if i % 8 == 0:
                first += 1;
            positions.append([first, 0])

        for i in range(nrOfPositions):  # for every position on the board,
            if i % 8 == 0:
                second = 1;
            positions[i][1] = second
            second += 1

        return positions

    'Set up the pieces'

    def setPositions(self):

        for i in range(8):
            self.standing.append(self.positions[i])

    'Picture the board'

    def pictureBoard(self):
        print("\n")
        print("-------------------------------------------------")

        for rowNr in range(8):
            row = "|"
            for piece in self.standing:
                x = piece[0]
                if x == rowNr + 1:
                    row += "  X  |"
                else:
                    row += "     |"
            print(row)
            if rowNr != 8 - 1:
                print("+-----+-----+-----+-----+-----+-----+-----+-----+")
            else:
                print("-------------------------------------------------")

    'Move'

    def move(self):
        piece = self.currentPiece-1
        if self.standing[piece][0] == 8:
            self.standing[piece][0] = 0
        self.standing[piece][0] += 1
        self.nrOfMoves += 1
        print(self.nrOfMoves, ": piece nr", self.currentPiece, "moved to", self.standing[piece], "\n-----------------------")

    # row-wise
    def checkRow(self):

        threatened = False
        row = self.standing[self.currentPiece-1][0]
        for i in range(8):  # Default: 8x8!

            if i + 1 != self.standing[self.currentPiece-1][1]:
                #print([row, i + 1])
                if [row, i + 1] in self.standing:
                    threatened = True
        return threatened

    # up-left
    def checkUpLeft(self):

        threatened = False
        row = self.standing[self.currentPiece-1][0]
        col = self.standing[self.currentPiece-1][1]
        while row > 1 and col > 1:
            row -= 1
            col -= 1
            #print([row, col])
            if [row, col] in self.standing:
                threatened = True
        return threatened

    # down-left
    def checkDownLeft(self):

        threatened = False
        row = self.standing[self.currentPiece-1][0]
        col = self.standing[self.currentPiece-1][1]
        while row < 8 and col > 1:
            row += 1
            col -= 1
            #print([row, col])
            if [row, col] in self.standing:
                threatened = True
        return threatened

    # up-right
    def checkUpRight(self):

        threatened = False
        row = self.standing[self.currentPiece-1][0]
        col = self.standing[self.currentPiece-1][1]
        #print("checking up right for piece with starting pos", col, ":")
        while row > 1 and col < 8:
            row -= 1
            col += 1
            #print([row, col])
            if [row, col] in self.standing:
                threatened = True
        return threatened

    # down-right
    def checkDownRight(self):

        threatened = False
        row = self.standing[self.currentPiece-1][0]
        col = self.standing[self.currentPiece-1][1]
        #print("checking down right for piece with starting pos", col, ":")
        while row < 8 and col < 8:
            row += 1
            col += 1
            #print([row, col])
            if [row, col] in self.standing:
                threatened = True
        return threatened

    def isThreatened(self):

        threatened = False
        if self.checkRow():
            #print(self.currentPiece, "is threatened row-wise")
            threatened = True
        if self.checkUpLeft():
            #print(self.currentPiece, "is threatened from upleft")
            threatened = True
        if self.checkDownLeft():
            #print(self.currentPiece, "is threatened from downleft")
            threatened = True
        if self.checkUpRight():
            #print(self.currentPiece, "is threatened from upright")
            threatened = True
        if self.checkDownRight():
            #print(self.currentPiece, "is threatened from downright")
            threatened = True

        return  threatened

    def moveUntilUnthreatened(self):

        if not self.isThreatened():
            print("piece nr", self.currentPiece, "is not threatened\n")
            return
        n = 0
        while n < 8:
            if self.isThreatened():
                self.move()
            else:
                break
            n += 1

        # piece is reset to original position
        if self.isThreatened():
            self.standing[self.currentPiece-1] = [1,self.currentPiece]

    def checkGameOver(self):

        gameOver = True
        self.pictureBoard()
        for piece in self.standing:
            self.currentPiece = piece[1]
            print("piece", self.currentPiece, "has position", piece)
            #if self.isThreatened:
                #print(self.currentPiece,"is threatened")
                #gameOver = False
            if self.checkRow():
                gameOver = False
            if self.checkDownRight() == True:
                gameOver = False
            if self.checkUpRight() == True:
                gameOver = False
            if self.checkDownLeft() == True:
                gameOver = False
            if self.checkUpLeft() == True:
                gameOver = False

        if gameOver:
            print("Game is Over!")
        return gameOver

    def play(self):

        nr = 1
        for k in range(8):
            self.currentPiece = nr
            self.move()
            for n in range(7):
                for i in range(8):
                    if self.currentPiece % 8 == 0:
                        self.currentPiece = 0
                    self.currentPiece += 1
                    if self.currentPiece != nr:
                        self.moveUntilUnthreatened()

                gameOver = self.checkGameOver()
                if not gameOver:
                    self.pictureBoard()
                    self.currentPiece = nr
                    self.move()
                else:
                    break

            if gameOver:
                self.pictureBoard()
                print("GAME OVER in", self.nrOfMoves, "moves")
                break

            nr += 1
            print("\n###################")
            print("#--PIECE", nr, "STARTS--")
            print("###################")

            self.resetBoard()
            g.pictureBoard()


if __name__ == '__main__':

    print("game is started")
    g = Game()
    g.play()

