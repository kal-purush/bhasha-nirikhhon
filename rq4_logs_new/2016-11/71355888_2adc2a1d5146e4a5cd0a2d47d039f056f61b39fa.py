import cv2
from model import PCAModel
import numpy as np
import math

class ChessBoard(object):
    def __init__(self, path):
        self.path = path
        self.image_core = cv2.cvtColor(cv2.imread(path), cv2.COLOR_BGR2RGB)
        self.pieces = ["rook", "knight", "bishop", "king", "queen", "pawn"]
        self.colors = ['black', 'white']
        self.data = self.load_training_set()
        self.board = self.init_board()
        self.EigenPieceChess = PCAModel()
        self.prepare_image()
        self.recognize()

    def init_board(self):
        board = [
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0],
            [0, 0, 0, 0, 0, 0, 0, 0]
        ]
        return board

    def prepare_image(self):
        image_gs = cv2.cvtColor(self.image_core, cv2.COLOR_RGB2GRAY)
        ret, image_bin = cv2.threshold(image_gs, 130, 255, cv2.THRESH_BINARY)
        img_bin = cv2.resize(image_bin, (240, 240))
        self.image_bin = img_bin

    def get_piece_color(self, image, label):
        B = 0
        W = 0
        color = 0
        for byte in image:
            if byte > 128:
                W = W + 1
            else:
                B = B + 1
        P = math.ceil(B/(B+W)*100)
        if P >= 25:
            color = -1
        elif P >= 6:
            color = 1
        else:
            color = 0
        if label == "queen":
            if P < 31:
                color = 1
        return color

    def load_training_set(self):
        data = []
        for color in self.colors:
            for piece in self.pieces:
                    path = './images/'+color+'/'+piece+'.png'
                    label = piece
                    component_core = cv2.imread(path)
                    component_gs = cv2.cvtColor(component_core, cv2.COLOR_RGB2GRAY)
                    ret, component_bin = cv2.threshold(component_gs, 130, 255, cv2.THRESH_BINARY)
                    component = cv2.resize(component_bin, (30, 30))
                    vector = np.array(component).ravel()
                    data.append({'label': label, 'sample': vector})
            return data

    def recognize(self):
                self.EigenPieceChess.train(self.data)
                for i in range(8):
                    for j in range(8):
                        vector = np.array(self.image_bin[i*30:(i+1)*30, j*30:(j+1)*30]).ravel()
                        label, res = self.EigenPieceChess.classify(vector)
                        self.board[i][j] = (self.pieces.index(label)+1)*self.get_piece_color(vector, label)

    def get_board(self):
        return self.board