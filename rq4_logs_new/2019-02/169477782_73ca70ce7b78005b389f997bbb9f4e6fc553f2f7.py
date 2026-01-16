import pygame  # use pygame library for gui (www.pygame.org)
import sys
import time

WHITE = (255, 255, 255)  # define colours for pygame in (r, g, b) format
BLACK = (0, 0, 0)
BACK_COLOUR = WHITE  # background colour
GRID_COLOUR = BLACK  # grid colour
SPRITE_COLOUR = BLACK  # colour of live cells

WIDTH = 640  # set width and height of game window (should be multiples of 16)
HEIGHT = 480
DEFAULT_BLOCKSIZE = 16  # grid block size (should be 8 or 16)
BLOCKSIZE = DEFAULT_BLOCKSIZE

FPS = 30 # game frames per second
IT_LENGTH = 0.2 # time period between each game iteration in seconds

class LiveCell(pygame.sprite.Sprite):  # sprite for a live cell

    def __init__(self, x_pos, y_pos):
        pygame.sprite.Sprite.__init__(self)  # need to initialize the sprite
        self.draw_cell()  # draw cell at current scale
        (self.rect.x, self.rect.y) = (x_pos, y_pos)  # set position
            
    def zoom_redraw(self, zoom):
    # redraw live cells to the correct size when the zoom is changed, maintaining screen centre
        x_pos, y_pos = self.rect.x, self.rect.y  # get previous position
        self.draw_cell()  # draw cell at current scale
        midwidth, midheight = int(WIDTH / 2), int(HEIGHT / 2)  # get positions of grid mid-lines to update sprite positions
        if zoom == 1:
            prev_blocksize = int(BLOCKSIZE / 2)  # need previous blocksize to calculate new position
        else:
            prev_blocksize = BLOCKSIZE * 2
        x_dist, y_dist = int(x_pos / prev_blocksize) - int(midwidth / prev_blocksize), int(y_pos / prev_blocksize) - int(midheight / prev_blocksize)  # x and y separations from middle in numbers of blocks
        self.rect.x = midwidth + BLOCKSIZE * (x_dist)  # update new positions
        self.rect.y = midheight + BLOCKSIZE * (y_dist)
            
    def draw_cell(self):
    # draw cell at the current scale
        self.image = pygame.Surface((BLOCKSIZE, BLOCKSIZE))  # represent live cell by square of specified colour
        self.image.fill(SPRITE_COLOUR)
        self.rect = self.image.get_rect()  # get rectangular area on screen in which sprite is contained
    
class GameOfLife(object):

    def __init__(self):
    # initialize the game
        pygame.init()  # initialize pygame
        pygame.display.set_caption("Game of Life")  # set title
        self.screen = pygame.display.set_mode((WIDTH, HEIGHT))  # create a pygame window
        self.clock = pygame.time.Clock()  # initialize clock to control game speed
        self.config_stage = False

    def reset(self):
    # start a new game
        global BLOCKSIZE
        BLOCKSIZE = DEFAULT_BLOCKSIZE  # reset zoom to default
        # self.screen.fill(BACK_COLOUR)
        # self.draw_grid()
        self.cell_group = pygame.sprite.Group()  # this group holds all live cell sprites
        pygame.display.update()
        self.config_stage = True
        self.gameloop()

    def cell_kill(self, x_pos, y_pos):
    # delete cell at input position
        for cell in self.cell_group:
            if cell.rect.collidepoint(x_pos, y_pos):
                cell.kill()
                del cell
                return True
        return False  # return true if cell deleted and false otherwise
        
    def add_delete_cell(self, pos):
    # add or delete live cell at clicked position
        x_pos = pos[0] - (pos[0] % BLOCKSIZE)  # get top-left coordinates of cell
        y_pos = pos[1] - (pos[1] % BLOCKSIZE)
        if not self.cell_kill(x_pos, y_pos):  # delete live cell if one exists in clicked position, and if not create one
            cell = LiveCell(x_pos, y_pos)
            self.cell_group.add(cell)    

    def get_cell_neighbours(self, pos):
    # return list of coordinates of 8 neighbouring cells
        neighbours = []  # initialize list to store neighbours
        x, y = pos[0], pos[1]  # get own coordinates
        for i in (BLOCKSIZE, 0, -BLOCKSIZE):  # loop through neighbours and save coordinates
            for j in (BLOCKSIZE, 0, -BLOCKSIZE):
                neighbours.append((x + i, y + j))
        neighbours.remove((x, y))  # remove self from neighbours list
        return neighbours
        
    def gameloop(self):
    # main loop of the game
        while True:
            iteration = time.time() + IT_LENGTH  # only perform game iteration in periods of IT_LENGTH
            while time.time() < iteration:
                self.clock.tick(FPS)  # run loop at specified speed
                self.events()  # check for events
                self.update()  # update the screen
            if not self.config_stage:  # perform next game iteration when in the active stage
                self.iter()
            
    def iter(self):
    # perform one iteration of game of life
        neighbours, live_cells = {}, {}  # initialize empty dicts to store live cells and their neighbours
        for cell in self.cell_group:  # store live cells and initialize counts to zero
            live_cells[(cell.rect.x, cell.rect.y)] = 0
        for key in live_cells:
            for neighbour in self.get_cell_neighbours(key):
                if neighbour in live_cells:
                    live_cells[neighbour] += 1
                elif neighbour in neighbours:
                    neighbours[neighbour] += 1
                else:
                    neighbours[neighbour] = 1
        self.rules(live_cells, neighbours)      

    def events(self):
    # listen for events in game loop
        for event in pygame.event.get():  # quit program if a quit event is found
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()
            if self.config_stage and event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:  # int 1 corresponds to left-click 
                self.add_delete_cell(event.pos)
            if self.config_stage and event.type == pygame.KEYDOWN and event.key == pygame.K_RETURN:
                self.config_stage = False
                return
            if not self.config_stage and event.type == pygame.KEYDOWN and event.key == pygame.K_RETURN:
                self.reset()
            if event.type == pygame.KEYUP and (event.key == pygame.K_m or event.key == pygame.K_p):
                self.zoom(event.key)
        self.scroll()
        # self.zoom()
                
    def update(self):
    # draw updates after iterations, scrolling, etc. (only need to update sections within one square of a sprite)
        self.screen.fill(BACK_COLOUR)
        self.draw_grid()  # draw grid on current screen
        self.cell_group.draw(self.screen)
        pygame.display.update()

    def draw_grid(self):
    # helper function to draw grid
        for i in range(0, WIDTH, BLOCKSIZE):
            pygame.draw.line(self.screen, GRID_COLOUR, (i, 0), (i, HEIGHT))
        for j in range(0, HEIGHT, BLOCKSIZE):
            pygame.draw.line(self.screen, GRID_COLOUR, (0, j), (WIDTH, j))
            
    def scroll(self):
    # helper function for scrolling using keyboard arrows
        x_vel, y_vel = 0, 0  # initialize x and y scrolling velocities
        keys = pygame.key.get_pressed()
        if keys[pygame.K_LEFT]:  # update velocities based on keyboard input
            x_vel = BLOCKSIZE
        if keys[pygame.K_RIGHT]:
            x_vel = -BLOCKSIZE
        if keys[pygame.K_UP]:
            y_vel = BLOCKSIZE
        if keys[pygame.K_DOWN]:
            y_vel = -BLOCKSIZE
        for cell in self.cell_group:  # move all cells to perform scrolling
            cell.rect.x += x_vel
            cell.rect.y += y_vel
        
    def zoom(self, key):
    # helper function to zoom in and out using keyboard
        zoom = 0
        update_cells = False
        global BLOCKSIZE
        if key == pygame.K_m and BLOCKSIZE > 2:
            BLOCKSIZE = int(BLOCKSIZE / 2)
            update_cells = True
            zoom = -1
        if key == pygame.K_p and BLOCKSIZE < 16:
            BLOCKSIZE = BLOCKSIZE * 2
            update_cells = True
            zoom = 1
        if update_cells:  # only redraw sprites if the zoom has been changed
            for cell in self.cell_group:
                cell.zoom_redraw(zoom)
            
    def rules(self, live_cells, neighbours):
    # rules for updating cells
        for key in neighbours:
            if neighbours[key] == 3:
                x_pos, y_pos = key[0], key[1]
                cell = LiveCell(x_pos, y_pos)
                self.cell_group.add(cell)
        for key in live_cells:
            if live_cells[key] < 2 or live_cells[key] > 3:
                x_pos, y_pos = key[0], key[1]
                self.cell_kill(x_pos, y_pos)

print("\n  \
Welcome to Game of Life \n\n  \
Left-click squares to select cells in your initial configuration  \n\n  \
Use the keyboard arrows to scroll across the grid \n\n  \
Zoom in or out by pressing 'p' or 'm' \n\n  \
The game starts when your press the \'enter\' key \n\n  \
Press \'enter\' again at any time to start a new game")

                
gol = GameOfLife()
gol.reset()