class Player:
    def __init__(self, name, symbol, board):
        self.__name = name
        self.__symbol = symbol
        self.__board = board

    @property
    def name(self):
        return self.__name

    @property
    def symbol(self):
        return self.__symbol

    @property
    def board(self):
        return self.__board

    def make_move(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} needs to implement this method")