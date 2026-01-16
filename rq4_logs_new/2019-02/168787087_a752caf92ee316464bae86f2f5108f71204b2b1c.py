from enum import Enum


class Tile:
    """ A tile is identified by whether it is red or white and by whether the dot on it is filled or empty.
        Thus, we use the following legend:
        R = Red
        W = White
        F = Filled
        E = Empty
    """
    class Color(Enum):
        red = 'R'
        white = 'W'

    class DotState(Enum):
        filled = 'F'
        empty = 'E'

    def __init__(self, color, dotState, playerOwner, cardOwner):
        self.color = color
        self.dotState = dotState
        self.playerOwner = playerOwner
        self.cardOwner = cardOwner

    def __str__(self):
        return self.color.value + self.dotState.value

    def __repr__(self):
        return self.__str__()


class Side:

    def __init__(self, tile1, tile2):
        self.tile1 = tile1
        self.tile2 = tile2

    def __str__(self):
        return '||' + self.tile1.__str__() + ' | ' + self.tile2.__str__() + '||'


class Card:
    class Orientation(Enum):
        right = 1
        down = 2
        left = 3
        up = 4

    NBR_CARDS = 24
    NBR_ROTATION_CODES = 8

    def __init__(self, owner, rotationCode=1):
        self.owner = owner
        self.side1 = Side(Tile(Tile.Color.red, Tile.DotState.filled, self.owner, self),
                     Tile(Tile.Color.white, Tile.DotState.empty, self.owner, self))
        self.side2 = Side(Tile(Tile.Color.red, Tile.DotState.empty, self.owner, self),
                          Tile(Tile.Color.white, Tile.DotState.filled, self.owner, self))
        self.rotationCode = rotationCode
        if self.rotationCode <= self.NBR_ROTATION_CODES / 2:
            self.activeSide = self.side1
            self.orientation = self.Orientation(self.rotationCode)
        else:
            self.activeSide = self.side2
            self.orientation = self.Orientation(self.rotationCode - self.NBR_ROTATION_CODES / 2)

    def __str__(self):
        # NOTE: This specific "toString" method should only be used for debug purposes
        return  "Info on player " + self.owner.__str__() + "'s card:\n" + \
                "Side 1: " + self.side1.__str__() + "\n" + \
                "Side 2: " + self.side2.__str__() + "\n" + \
                "Rotation Code: " + self.rotationCode.__str__() + "\n" + \
                "Active Side: " + ("Side 1" if self.activeSide == self.side1 else "Side 2") + "\n" + \
                "Orientation: " + self.orientation.__str__()


class Board:
    DIMENSIONS_X_Y = (8, 12)

    def __init__(self):
        self.board = [[Tile() for x in range(self.DIMENSIONS_X_Y[0])] for y in range (self.DIMENSIONS_X_Y[1])]

    def __str__(self):
        for row in self.board:
            for colVal in row:
                colVal
            print()


c = Card(1)
print(c, "\n")
c2 = Card(2, 7)
print(c2)