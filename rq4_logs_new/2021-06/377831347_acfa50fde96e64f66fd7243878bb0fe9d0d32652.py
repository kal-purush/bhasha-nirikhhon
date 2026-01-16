import pygame as pg
import boardCube as bCube
import piece


class Board:
    board = [
        [0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0],
        [0,0,0,0,0,0,0,0]
    ]
    clickedCubeI = -1
    clickedCubeJ = -1

    def __init__(self): 
        cubeFiles = [ 'A','B','C','D','E','F','G','H']
        cubeRanks = [ '8','7','6','5','4','3','2','1']
        self.boardCubes = [[ bCube.boardCube(80, cubeFiles[i-1], cubeRanks[j-1], i*80, j*80, (0,0,0) if i%2==0 and j%2==1 or j%2==0 and i%2==1 else (255,255,255)) for j in range(1,9)] for i in range(1,9)]

    def initialiseGame(self):
        #Peons
        for i in range(0,8):
            self.boardCubes[i][1].cubePiece = piece.Piece("Peon","Black")
            self.boardCubes[i][6].cubePiece = piece.Piece("Peon","White")
            self.board[i][1] = 1
            self.board[i][6] = 1
        #Rooks
        self.boardCubes[0][0].cubePiece = piece.Piece("Rook","Black")
        self.boardCubes[7][0].cubePiece = piece.Piece("Rook","Black")
        self.boardCubes[0][7].cubePiece = piece.Piece("Rook","White")
        self.boardCubes[7][7].cubePiece = piece.Piece("Rook","White")
        self.board[0][0] = 1
        self.board[7][0] = 1
        self.board[0][7] = 1
        self.board[7][7] = 1
        #Knights
        self.boardCubes[1][0].cubePiece = piece.Piece("Knight","Black")
        self.boardCubes[6][0].cubePiece = piece.Piece("Knight","Black")
        self.boardCubes[1][7].cubePiece = piece.Piece("Knight","White")
        self.boardCubes[6][7].cubePiece = piece.Piece("Knight","White")
        self.board[1][0] = 1
        self.board[6][0] = 1
        self.board[1][7] = 1
        self.board[6][7] = 1
        #Bishops
        self.boardCubes[2][0].cubePiece = piece.Piece("Bishop","Black")
        self.boardCubes[5][0].cubePiece = piece.Piece("Bishop","Black")
        self.boardCubes[2][7].cubePiece = piece.Piece("Bishop","White")
        self.boardCubes[5][7].cubePiece = piece.Piece("Bishop","White")
        self.board[2][0] = 1
        self.board[5][0] = 1
        self.board[2][7] = 1
        self.board[5][7] = 1
        #Queens
        self.boardCubes[3][0].cubePiece = piece.Piece("Queen","Black")
        self.boardCubes[3][7].cubePiece = piece.Piece("Queen","White")
        self.board[3][0] = 1
        self.board[3][7] = 1
        #Kings
        self.boardCubes[4][0].cubePiece = piece.Piece("King","Black")
        self.boardCubes[4][7].cubePiece = piece.Piece("King","White")
        self.board[4][0] = 1
        self.board[4][7] = 1


    def drawBoard(self, screen):
        pg.draw.line(screen,(0,0,0),(80,79),(720,79),1)
        pg.draw.line(screen,(0,0,0),(720,79),(720,719),1)
        pg.draw.line(screen,(0,0,0),(80,720),(720,720),1)
        pg.draw.line(screen,(0,0,0),(79,79),(79,720),1)
        for i in range (0,8):
            for j in range (0,8):
                 self.boardCubes[i][j].drawCube(screen)

    def click(self, pos):  
        for i in range (0,8):
            for j in range (0,8):
                if( self.boardCubes[i][j].checkIfClicked(pos) ):
                    self.boardCubes[self.clickedCubeI][self.clickedCubeJ].isClicked = False
                    if(self.boardCubes[self.clickedCubeI][self.clickedCubeJ].cubePiece.pieceColor != "None"):
                        if(self.clickedCubeI != -1 and self.clickedCubeJ != -1 ):
                            if(self.boardCubes[i][j].cubePiece.pieceColor != self.boardCubes[self.clickedCubeI][self.clickedCubeJ].cubePiece.pieceColor):
                                if(self.boardCubes[self.clickedCubeI][self.clickedCubeJ].cubePiece.movePiece(self.board,i,j,self.clickedCubeI,self.clickedCubeJ)):
                                    self.boardCubes[i][j].cubePiece = self.boardCubes[self.clickedCubeI][self.clickedCubeJ].cubePiece
                                    self.boardCubes[self.clickedCubeI][self.clickedCubeJ].cubePiece = piece.Piece("None","None")
                                    self.boardCubes[i][j].isClicked = False
                                else:
                                    self.boardCubes[i][j].isClicked = False
                            else: 
                                self.boardCubes[i][j].isClicked = False
                    self.clickedCubeI = i if self.boardCubes[i][j].isClicked else -1
                    self.clickedCubeJ = j if self.boardCubes[i][j].isClicked else -1