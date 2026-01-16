import pygame
from pygame import gfxdraw
import math


class Piece:

    # Constructor
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    # Constructor
    def __init__(self, player, x, y):
        self.player = player
        self.color = (255, 0, 0) if player == 1 else (0, 0, 255)
        self.rect = pygame.rect.Rect(x, y, 7, 2)
        self.is_selected = False
        self.as_moved = False
        self.x = x
        self.y = y

    def move(self, x, y):
        self.as_moved = True
        self.rect = pygame.rect.Rect(x, y, 7, 2)
        self.x = x
        self.y = y

    def draw(self):
        gfxdraw.aacircle(screen, self.x, self.y, 7, self.color)
        gfxdraw.filled_circle(screen, self.x, self.y, 7, self.color)

    def set_selected(self):
        self.is_selected = True
        self.color = (0, 255, 0)

    def deselect(self):
        self.is_selected = False
        self.color = (255, 0, 0) if self.player == 1 else (0, 0, 255)


board = [[None] * 3, [None] * 3, [None] * 3]
points_list = [30, 150, 270]
player = 1
won = False
screen = None
running = True


def initiate_game():
    global screen

    screen = pygame.display.set_mode((300, 300))
    screen.set_alpha(None)
    pygame.display.set_caption('Corners game')
    screen.fill(color=(234, 212, 252))
    pygame.font.init()
    pygame.display.flip()

    board[0][0] = Piece(1, 30, 30)
    board[1][0] = Piece(1, 30, 150)
    board[2][0] = Piece(1, 30, 270)
    board[0][2] = Piece(2, 270, 30)
    board[1][2] = Piece(2, 270, 150)
    board[2][2] = Piece(2, 270, 270)

    pygame.display.flip()


def draw_background():
    # Lines of the game
    pygame.draw.line(surface=screen, color=(0, 0, 0),
                     start_pos=(30, 30), end_pos=(30, 270))
    pygame.draw.line(surface=screen, color=(0, 0, 0),
                     start_pos=(30, 30), end_pos=(270, 30))
    pygame.draw.line(surface=screen, color=(0, 0, 0),
                     start_pos=(270, 30), end_pos=(270, 270))
    pygame.draw.line(surface=screen, color=(0, 0, 0),
                     start_pos=(30, 270), end_pos=(270, 270))
    pygame.draw.line(surface=screen, color=(0, 0, 0),
                     start_pos=(30, 270), end_pos=(270, 270))
    pygame.draw.line(surface=screen, color=(0, 0, 0),
                     start_pos=(150, 30), end_pos=(150, 270))
    pygame.draw.line(surface=screen, color=(0, 0, 0),
                     start_pos=(30, 150), end_pos=(270, 150))


def draw_board():
    for row in board:
        for cell in row:
            if cell is not None:
                cell.draw()


def get_closest_piece(x, y):
    grid_x = 0
    grid_y = 0

    for point_y in points_list:
        grid_x = 0
        for point_x in points_list:
            if math.sqrt(math.pow(x-point_x, 2)+math.pow(y-point_y, 2)) < 30:
                return board[grid_y][grid_x], grid_x, grid_y
            grid_x = grid_x+1
        grid_y = grid_y+1
    return None, -1, -1


def is_any_piece_selected():
    grid_x = 0
    grid_y = 0
    for point_y in points_list:
        grid_x = 0
        for point_x in points_list:
            if board[grid_y][grid_x] is not None and board[grid_y][grid_x].is_selected:
                return True, board[grid_y][grid_x], grid_x, grid_y
            grid_x = grid_x+1
        grid_y = grid_y+1
    return False, None, grid_x, grid_y


def deselect_all_pieces():
    for row in board:
        for cell in row:
            if cell is not None:
                cell.deselect()


def is_valid_move(selected_grid_x, selected_grid_y, grid_x, grid_y):
    if selected_grid_x == grid_x and selected_grid_y != grid_y or selected_grid_x != grid_x and selected_grid_y == grid_y:
        return True
    return False


def get_player_pieces():
    player_pieces = []
    for row in board:
        for cell in row:
            if cell is not None and cell.player == player and cell.as_moved == True:
                player_pieces.append(cell)
    return player_pieces


def as_win():
    player_pieces = []
    same_x = 0
    same_y = 0
    x = 0
    y = 0

    player_pieces = get_player_pieces()

    if len(player_pieces) <= 0:
        return False

    for piece in player_pieces:
        if same_x == 0:
            x = piece.x

        if same_y == 0:
            y = piece.y

        if piece.x == x:
            same_x = same_x + 1

        if piece.y == y:
            same_y = same_y + 1
    return True if same_y == 3 or same_x == 3 else False


initiate_game()
while running:

    my_font = pygame.font.Font(None, 30)

    # for loop through the event queue
    for event in pygame.event.get():

        # Check for QUIT event
        if event.type == pygame.QUIT:
            running = False

        left, middle, right = pygame.mouse.get_pressed()

        if left:
            x = pygame.mouse.get_pos()[0]
            y = pygame.mouse.get_pos()[1]
            is_any_selected, selected_piece, selected_grid_x, selected_grid_y = is_any_piece_selected()
            piece, grid_x, grid_y = get_closest_piece(x, y)

            if is_any_selected and selected_piece.player == player and piece == None and grid_x != -1 and grid_y != -1 and is_valid_move(selected_grid_x, selected_grid_y, grid_x, grid_y):
                selected_piece.move(points_list[grid_x], points_list[grid_y])
                board[grid_y][grid_x] = selected_piece
                board[grid_y][grid_x].deselect()
                board[selected_grid_y][selected_grid_x] = None
                if as_win():
                    won = True
                else:
                    player = 1 if player == 2 else 2

            # if there isnt a piece selected
            else:
                if piece != None and not piece.is_selected and piece.player == player:
                    deselect_all_pieces()
                    piece.set_selected()

        screen.fill((255, 255, 255))
        draw_background()
        draw_board()

        if won:
            label = my_font.render("Player "+str(player)+" as won",
                                   True, (255, 0, 0) if player == 1 else (0, 0, 255))
            screen.blit(label, (70, 7))
        else:
            label = my_font.render("Player "+str(player)+" turn",
                                   True, (255, 0, 0) if player == 1 else (0, 0, 255))
            screen.blit(label, (90, 7))

        pygame.display.flip()
        pygame.time.Clock().tick(60)