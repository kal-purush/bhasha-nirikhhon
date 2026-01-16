import pygame
import random

# creating the data structure for pieces
# setting up global vars
# functions
# - create_grid
# - draw_grid
# - draw_window
# - rotating shape in main
# - setting up the main

"""
10 x 20 square grid
shapes: S, Z, I, O, J, L, T
represented in order by 0 - 6
"""

pygame.font.init()

# GLOBALS VARS
s_width = 800
s_height = 700
play_width = 300  # meaning 300 // 10 = 30 width per block
play_height = 600  # meaning 600 // 20 = 20 height per block
block_size = 30

top_left_x = (s_width - play_width) // 2
top_left_y = s_height - play_height


# SHAPE FORMATS

S = [['.....',
      '.....',
      '..00.',
      '.00..',
      '.....'],
     ['.....',
      '..0..',
      '..00.',
      '...0.',
      '.....']]

Z = [['.....',
      '.....',
      '.00..',
      '..00.',
      '.....'],
     ['.....',
      '..0..',
      '.00..',
      '.0...',
      '.....']]

I = [['..0..',
      '..0..',
      '..0..',
      '..0..',
      '.....'],
     ['.....',
      '0000.',
      '.....',
      '.....',
      '.....']]

O = [['.....',
      '.....',
      '.00..',
      '.00..',
      '.....']]

J = [['.....',
      '.0...',
      '.000.',
      '.....',
      '.....'],
     ['.....',
      '..00.',
      '..0..',
      '..0..',
      '.....'],
     ['.....',
      '.....',
      '.000.',
      '...0.',
      '.....'],
     ['.....',
      '..0..',
      '..0..',
      '.00..',
      '.....']]

L = [['.....',
      '...0.',
      '.000.',
      '.....',
      '.....'],
     ['.....',
      '..0..',
      '..0..',
      '..00.',
      '.....'],
     ['.....',
      '.....',
      '.000.',
      '.0...',
      '.....'],
     ['.....',
      '.00..',
      '..0..',
      '..0..',
      '.....']]

T = [['.....',
      '..0..',
      '.000.',
      '.....',
      '.....'],
     ['.....',
      '..0..',
      '..00.',
      '..0..',
      '.....'],
     ['.....',
      '.....',
      '.000.',
      '..0..',
      '.....'],
     ['.....',
      '..0..',
      '.00..',
      '..0..',
      '.....']]

shapes = [S, Z, I, O, J, L, T]
shape_colors = [(0, 255, 0), (255, 0, 0), (0, 255, 255), (255, 255, 0), (255, 165, 0), (0, 0, 255), (128, 0, 128)]
# index 0 - 6 represent shape


class Piece(object):
    def __init__(self, x, y, shape):
        self.x = x
        self.y = y
        self.shape = shape
        self.color = shape_colors[shapes.index(shape)]
        self.rotation = 0


def create_grid(locked_pos={}):
    grid = [[(0,0,0) for _ in range(10)] for _ in range(20)]

    for i in range(len(grid)):
        for j in range(len(grid[i])):
            if (j,i) in locked_pos:
                c = locked_pos[(j,i)]
                grid[i][j] = c
    return grid


def convert_shape_format(shape):
    positions = []
    format = shape.shape[shape.rotation % len(shape.shape)]

    for i, line in enumerate(format):
        row = list(line)
        for j, column in enumerate(row):
            if column == '0':
                positions.append((shape.x + j, shape.y + i))

    for i, pos in enumerate(positions):
        positions[i] = (pos[0] - 2, pos[1] - 4)

    return positions


def valid_space(shape, grid):
    accepted_pos = [[(j, i) for j in range(10) if grid[i][j] == (0,0,0)] for i in range(20)]
    accepted_pos = [j for sub in accepted_pos for j in sub]

    formatted = convert_shape_format(shape)

    for pos in formatted:
        if pos not in accepted_pos:
            if pos[1] > -1:
                return False

    return True


def check_lost(positions):
    for pos in positions:
        x, y = pos
        if y < 1:
            return True
    return False


def get_shape(bag):
    shapes_copy = [S, Z, I, O, J, L, T]
    if len(bag) <= 7:
        random.shuffle(shapes_copy)
        for item in shapes_copy:
            bag.append(item)
    return Piece(5, 0, bag.pop(0))


def draw_text_middle(surface, text, size, color):
    font = pygame.font.SysFont("comicsans", size, bold=True)
    label = font.render(text, 1, color)

    surface.blit(label, (top_left_x + play_width/2 - label.get_width()/2, top_left_y + play_height/2 - label.get_height()/2))


def draw_grid(surface, grid):
    sx = top_left_x
    sy = top_left_y

    for i in range(len(grid)):
        pygame.draw.line(surface, (128,128,128), (sx, sy+ i*block_size), (sx+play_width, sy+ i*block_size))
        for j in range(len(grid[i])):
            pygame.draw.line(surface, (128,128,128), (sx + j*block_size, sy), (sx + j*block_size, sy+play_height))


def clear_rows(grid, locked):
    inc = 0
    for i in range(len(grid)-1, -1, -1):
        row = grid[i]
        if (0,0,0) not in row:
            inc += 1
            ind = i
            for j in range(len(row)):
                try:
                    del locked[(j,i)]
                except:
                    continue

    if inc > 0:
        for key in sorted(list(locked), key = lambda x: x[1])[::-1]:
            x, y = key
            if y < ind:
                newKey = (x, y + inc)
                locked[newKey] = locked.pop(key)

    return inc


def score_from_clearing(number_of_cleared_lines):
    if number_of_cleared_lines >= 4:   # tetris
        return 800
    elif number_of_cleared_lines == 3:  # triple
        return 500
    elif number_of_cleared_lines == 2:  # double
        return 300
    elif number_of_cleared_lines == 1: #single
        return 100
    else:
        return 0


def draw_next_shape(shape: list, surface):
    font = pygame.font.SysFont('comicsans', 20)
    label = font.render('Next', 1, (255,255,255))

    sx = top_left_x + play_width + 50
    sy = top_left_y + play_height/2 - 310
    # format = shape.shape[shape.rotation % len(shape.shape)]

    # for i, line in enumerate(format):
    #     row = list(line)
    #     for j, column in enumerate(row):
    #         if column == '0':
    #             pygame.draw.rect(surface, shape.color,
    #                              (sx + j*block_size, sy + i*block_size, block_size, block_size), 0)

    for index, one_shape in enumerate(shape):
        copy_sx = sx
        copy_sy = sy + (index*block_size*2.1)
        one_shape_formatted = one_shape.shape[one_shape.rotation % len(one_shape.shape)]
        for i, line in enumerate(one_shape_formatted):
            row = list(line)
            for j, column in enumerate(row):
                if column == '0':
                    pygame.draw.rect(surface, one_shape.color,
                                     (copy_sx + j*block_size*0.7, copy_sy + i*block_size*0.7, block_size*0.7, block_size*0.7), 0)

    surface.blit(label, (sx + 10, sy - 30))


def draw_hold(shape, surface):
    font = pygame.font.SysFont('comicsans', 20)
    label = font.render('Hold', 1, (255,255,255))

    sx = top_left_x - 100
    sy = top_left_y + play_height/2 - 310
    format = shape.shape[shape.rotation % len(shape.shape)]

    for i, line in enumerate(format):
        row = list(line)
        for j, column in enumerate(row):
            if column == '0':
                pygame.draw.rect(surface, shape.color,
                                 (sx + j*block_size*0.7, sy + i*block_size*0.7, block_size*0.7, block_size*0.7), 0)

    surface.blit(label, (sx + 10, sy - 30))


def update_score(nscore):
    with open("scores.txt", "r") as f:
        lines = f.readlines()
        score = lines[0].strip()

    with open("scores.txt", "w") as f:
        if int(score) > nscore:
            f.write(str(score))
        else:
            f.write(str(nscore))


def max_score():
    with open("scores.txt", "r") as f:
        lines = f.readlines()
        score = lines[0].strip()
    return score


def draw_window(surface, grid, score=0, last_score=0):
    surface.fill((0,0,0))

    pygame.font.init()
    font = pygame.font.SysFont('comicsans', 60)
    label = font.render('Tetris', 1, (255,255,255))

    surface.blit(label, (top_left_x + play_width/2 - label.get_width()/2, 30 ))
    # current score
    font = pygame.font.SysFont('comicsans', 20)
    label = font.render('Score: ' + str(score), 1, (255,255,255))

    sx = top_left_x + play_width + 50
    sy = top_left_y + play_height/2 - 100

    surface.blit(label, (sx + 10, sy + 200))
    # last score
    label = font.render('High Score: ' + last_score, 1, (255,255,255))

    sx = top_left_x + play_width + 50
    sy = top_left_y + play_height/2 - 60

    surface.blit(label, (sx + 10, sy + 200))

    for i in range(len(grid)):
        for j in range(len(grid[i])):
            pygame.draw.rect(surface, grid[i][j],
                             (top_left_x + j*block_size, top_left_y + i*block_size, block_size, block_size), 0)

    pygame.draw.rect(surface, (255,0,0), (top_left_x, top_left_y, play_width, play_height), 5)

    draw_grid(surface, grid)


def main(win):
    last_score = max_score()
    locked_positions = {}
    grid = create_grid(locked_positions)

    change_piece = False
    run = True

    bag = []

    current_piece = get_shape(bag)
    next_piece = [get_shape(bag) for i in range(5)]
    held_piece = None
    was_holding = False

    clock = pygame.time.Clock()
    fall_time = 0
    fall_speed = 0.7
    level_time = 0
    score = 0
    counting_before_locking = 0

    while run:
        grid = create_grid(locked_positions)
        fall_time += clock.get_rawtime()
        level_time += clock.get_rawtime()
        clock.tick()

        if level_time/1000 > 5:
            level_time = 0
            if fall_speed > 0.12:
                fall_speed -= 0.005

        if fall_time/1000 > fall_speed:
            fall_time = 0
            current_piece.y += 1
            if not(valid_space(current_piece, grid)) and current_piece.y > 0:
                counting_before_locking += 1
                current_piece.y -= 1
                if counting_before_locking > 3:
                    change_piece = True
                    counting_before_locking = 0

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                run = False
                pygame.display.quit()

            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_a:
                    current_piece.x -= 1
                    if not (valid_space(current_piece, grid)):
                        current_piece.x += 1
                if event.key == pygame.K_d:
                    current_piece.x += 1
                    if not (valid_space(current_piece, grid)):
                        current_piece.x -= 1
                if event.key == pygame.K_s:
                    current_piece.y += 1
                    if not (valid_space(current_piece, grid)):
                        current_piece.y -= 1
                if event.key == pygame.K_w:  # hard drop
                    while valid_space(current_piece, grid):
                        current_piece.y += 1
                    if not (valid_space(current_piece, grid)):
                        current_piece.y -= 1
                        counting_before_locking = 4
                if event.key == pygame.K_RIGHT:  # rotate clockwise
                    current_piece.rotation += 1
                    if not (valid_space(current_piece, grid)):
                        current_piece.rotation -= 1
                if event.key == pygame.K_DOWN:  # rotate counterclockwise
                    current_piece.rotation -= 1
                    if not (valid_space(current_piece, grid)):
                        current_piece.rotation += 1
                if event.key == pygame.K_q:  # hold
                    if not held_piece:
                        held_piece = current_piece
                        current_piece = next_piece.pop(0)
                        next_piece.append(get_shape(bag))
                        was_holding = True
                    else:
                        if not was_holding:
                            held_piece, current_piece = current_piece, held_piece
                            was_holding = True


        shape_pos = convert_shape_format(current_piece)

        for i in range(len(shape_pos)):
            x,y = shape_pos[i]
            if y > -1:
                grid[y][x] = current_piece.color

        if change_piece:
            for pos in shape_pos:
                p = (pos[0], pos[1])
                locked_positions[p] = current_piece.color
            current_piece = next_piece.pop(0)
            next_piece.append(get_shape(bag))
            change_piece = False
            was_holding = False
            score += score_from_clearing((clear_rows(grid, locked_positions)))

        draw_window(win, grid, score, last_score)
        draw_next_shape(next_piece, win)
        if held_piece:
            draw_hold(held_piece, win)
        pygame.display.update()

        if check_lost(locked_positions):
            draw_text_middle(win, "You Lost!", 80, (255,255,255))
            pygame.display.update()
            pygame.time.delay(5000)
            run = False
            update_score(score)
    pygame.display.quit()


def main_menu(win):
    run = True
    while run:
        win.fill((0,0,0))
        draw_text_middle(win, 'Press Any Key to Play', 60, (255,255,255))
        pygame.display.update()
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                run = False
            if event.type == pygame.KEYDOWN:
                main(win)
    pygame.display.quit()


win = pygame.display.set_mode((s_width, s_height))
pygame.display.set_caption('Tetris')
main_menu(win)  # start game