#This file stores all the current data about the game. It will keep a move log and determine the valid moves.

class GameState():
    def __init__(self):
        self.board = [   
            ["bR","bN","bB","bQ","bK","bB", "bN", "bR"],
            ["bp","bp","bp","bp","bp","bp","bp","bp"],
            ["--","--","--","--","--","--","--","--"],
            ["--","--","--","--","--","--","--","--"],
            ["--","--","--","--","--","--","--","--"],
            ["--","--","--","--","--","--","--","--"],
            ["wR","wN","wB","wQ","wK","wB", "wN", "wR"],
            ["wp","wp","wp","wp","wp","wp","wp","wp"],            
        ]

        self.whiteToMove = True #To check whose turn it is
        self.moveLog = [] 