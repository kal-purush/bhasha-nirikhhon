import pygame

GRID_MARGIN_SIZE = 10
CELL_SIZE = 32

SQUARE_GRID_MAGNITUDE = input('''Input square grid magnitude: ''')
while SQUARE_GRID_MAGNITUDE.isdigit() == False:
    input('''Input square grid magnitude: ''')
else:
    SQUARE_GRID_MAGNITUDE = int(SQUARE_GRID_MAGNITUDE)

WIDTH = HEIGHT = 2 * GRID_MARGIN_SIZE + CELL_SIZE * SQUARE_GRID_MAGNITUDE
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)


class square_grid():
    def __init__(self, surface, cellSize, magnitude):
        self.surface = surface
        self.cellSize = cellSize
        self.magnitude = magnitude

    def drawGrid(self):
        for horizontalLine in range(self.magnitude + 1):
            horizontalLine_Y_Cord = GRID_MARGIN_SIZE + horizontalLine * self.cellSize
            pygame.draw.line(self.surface, BLACK, (GRID_MARGIN_SIZE, horizontalLine_Y_Cord), (self.surface.get_width() - GRID_MARGIN_SIZE, horizontalLine_Y_Cord))
        for verticalLine in range(self.magnitude + 1):
            verticalLine_X_Cord =  GRID_MARGIN_SIZE + verticalLine * self.cellSize
            pygame.draw.line(self.surface, BLACK, (verticalLine_X_Cord, GRID_MARGIN_SIZE), (verticalLine_X_Cord, self.surface.get_height() - GRID_MARGIN_SIZE))


pygame.init()
screen = pygame.display.set_mode((WIDTH, HEIGHT))

screen.fill(WHITE)
grid = square_grid(surface=screen, cellSize=CELL_SIZE, magnitude=SQUARE_GRID_MAGNITUDE)
grid.drawGrid()

pygame.display.update()

while True:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            pygame.quit()