class gui_base:
    def __init__(self, gameState, play):
        self.canvasWidth = 1000
        self.canvasHeight = 600
        self.gameState = gameState
        self.play = play
        self.width = self.canvasHeight / (self.gameState.sizeBoard)
        self.fromTheEdge = self.width

    def hexagon(
        self, row, col
    ):  # draw a hexagon around position x,y with R, r as on https://en.wikipedia.org/wiki/Hexagon
        x = (col + row / 2) * self.width + self.fromTheEdge
        y = row * (math.sqrt(3) / 2 * self.width) + self.fromTheEdge
        r = self.width / 2
        R = r * 2 / math.sqrt(3)
        top = (x, y - R)
        bottom = (x, y + R)
        topLeft = (x - r, y - R / 2)
        bottomLeft = (x - r, y + R / 2)
        topRight = (x + r, y - R / 2)
        bottomRight = (x + r, y + R / 2)
        return [top, topRight, bottomRight, bottom, bottomLeft, topLeft]