# -------------------------------------------------------------------------------
# Name:        Constants
# Purpose:
#
# Author:      Tony
#
# Created:     22/07/2016
# Copyright:   (c) Tony 2016
# Licence:     <your licence>
# -------------------------------------------------------------------------------
""" Set of constants used in the game """

SIZE = 15                           # Number of squares on a side.
MID_ROW = SIZE/2                    # The coordinates of the center (starting) square.
MID_COL = SIZE/2
CELL_COUNT = SIZE*SIZE              # Number of squares on the board.
WIDTH = 40                          # Width of each square/tile
HEIGHT = 40                         # Height of each square/tile
MARGIN = 5                          # Margin between each cell

SPLASH_SIZE = [350, 250]
WINDOW_SIZE = [1300, 780]           # Window size

# position and size of buttons
LARGE_BUTTON = 40
SMALL_BUTTON = 30
BUTTON_X = WINDOW_SIZE[0]/2
BUTTON_Y = WINDOW_SIZE[1] - 70
BUTTON_START = BUTTON_X - 5
PLAY_BUTTON = BUTTON_START - LARGE_BUTTON * 2 - 1
ACCEPT_BUTTON = BUTTON_START + 1
CHALLENGE_BUTTON = PLAY_BUTTON - SMALL_BUTTON * 2 - 14
PASS_BUTTON = CHALLENGE_BUTTON - SMALL_BUTTON * 2
EXCHANGE_BUTTON = ACCEPT_BUTTON + LARGE_BUTTON * 2 - 6
QUIT_BUTTON = EXCHANGE_BUTTON + SMALL_BUTTON * 2

NEXT_BUTTON = BUTTON_X - 55
END_BUTTON = BUTTON_X - 55
BACK_BUTTON = BUTTON_X - 127
NEW_BUTTON = BUTTON_X + 17

NEXT_CLICK_X = BUTTON_X
PLAY_CLICK_X = PLAY_BUTTON + LARGE_BUTTON * 3 / 2
ACCEPT_CLICK_X = ACCEPT_BUTTON + LARGE_BUTTON
PASS_CLICK_X = PASS_BUTTON + SMALL_BUTTON * 3
CHALLENGE_CLICK_X = CHALLENGE_BUTTON + SMALL_BUTTON * 5 / 2
EXCHANGE_CLICK_X = EXCHANGE_BUTTON + SMALL_BUTTON / 2
QUIT_CLICK_X = QUIT_BUTTON + SMALL_BUTTON / 4
CLICK_Y = WINDOW_SIZE[1] - 50

# position of racks and tiles in racks
RACK_POS = [MARGIN*2, WINDOW_SIZE[0] - MARGIN*2 - 315]      # x-position of racks
RACK_Y = WINDOW_SIZE[1] - 70
RACK_XTILE = [RACK_POS[0] + 20, RACK_POS[1] + 20]
RACK_YTILE = WINDOW_SIZE[1] - 75

# position of timers
TIMER_POS = [BUTTON_X - 300, BUTTON_X + 300 - 75]
TIMER_Y = BUTTON_Y

# Design of board showing premium cells.
# Legend:
#    T = triple word
#    D = double word
#    t = triple letter
#    d = double letter
#    . = normal
PREMIUM_CELLS = (
    'T . . d . . . T . . . d . . T'
    '. D . . . t . . . t . . . D .'
    '. . D . . . d . d . . . D . .'
    'd . . D . . . d . . . D . . d'
    '. . . . D . . . . . D . . . .'
    '. t . . . t . . . t . . . t .'
    '. . d . . . d . d . . . d . .'
    'T . . d . . . D . . . d . . T'
    '. . d . . . d . d . . . d . .'
    '. t . . . t . . . t . . . t .'
    '. . . . D . . . . . D . . . .'
    'd . . D . . . d . . . D . . d'
    '. . D . . . d . d . . . D . .'
    '. D . . . t . . . t . . . D .'
    'T . . d . . . T . . . d . . T')

# Board square colours
TRIPLEWORD_COLOUR = (255, 0, 0)
DOUBLEWORD_COLOUR = (255, 192, 203)
TRIPLELETTER_COLOUR = (0, 0, 255)
DOUBLELETTER_COLOUR = (173, 216, 230)
NORMAL_COLOUR = (255, 255, 255)

TILE_WIDTH = 75
TILE_HEIGHT = 90

# Tile lift from rack in exchange state
TILE_LIFT = -50

# Playing board
BOARD_WIDTH = 15*(WIDTH + MARGIN) + MARGIN                  # width of playing board
BOARD_HEIGHT = 15*(HEIGHT + MARGIN) + MARGIN                # height of playing board
BOARD_X = (WINDOW_SIZE[0] - BOARD_WIDTH)/2                  # x-coordinate of scrabble board
BOARD = [BOARD_X, 0, BOARD_WIDTH, BOARD_HEIGHT]             # board rect details

# Square of upside down letters
SQUARE_SIZE = 10
SQUARE_WIDTH = 10*(WIDTH + MARGIN) + MARGIN                 # width of square
SQUARE_HEIGHT = 10*(HEIGHT + MARGIN) + MARGIN               # height of square
SQUARE_X = (WINDOW_SIZE[0] - SQUARE_WIDTH)/2                # x-coordinate
SQUARE_Y = (WINDOW_SIZE[1] - SQUARE_HEIGHT)/2               # y-coordinate
SQUARE = [SQUARE_X, SQUARE_Y, SQUARE_WIDTH, SQUARE_HEIGHT]  # square details

# Scoreboard and text area details
SCOREBOARD_WIDTH = BOARD_X - MARGIN*6                       # Width of scorebaords
SCOREBOARD = [[MARGIN*2, 70, SCOREBOARD_WIDTH, 250],
              [WINDOW_SIZE[0] - SCOREBOARD_WIDTH - MARGIN*2, 70, SCOREBOARD_WIDTH, 250]]

NAMES_RECT = [[MARGIN*2, 5, SCOREBOARD_WIDTH, 50],
              [WINDOW_SIZE[0] - SCOREBOARD_WIDTH - MARGIN*2, 5, SCOREBOARD_WIDTH, 50]]

MSGS_RECT = [[MARGIN*2, 500, SCOREBOARD_WIDTH, 100],
             [WINDOW_SIZE[0] - SCOREBOARD_WIDTH - MARGIN*2, 500,
              SCOREBOARD_WIDTH, 100]]

INSTR_RECT = [SQUARE_X - 10, SQUARE_Y + SQUARE_HEIGHT + 20, SQUARE_WIDTH + 20, 60]

# User input area
INPUT_WIDTH = SCOREBOARD_WIDTH - 50
INPUT_LETTER = [[MARGIN*2, RACK_Y - 280, INPUT_WIDTH, 60],
                [WINDOW_SIZE[0] - SCOREBOARD_WIDTH - MARGIN*2, RACK_Y - 280, INPUT_WIDTH, 60]]
INPUT_NAMES_WIDTH = 0
INPUT_NAMES = [[MARGIN*5, WINDOW_SIZE[1]/2, INPUT_NAMES_WIDTH, 40],
               [SQUARE_X + SQUARE_WIDTH + MARGIN*5, WINDOW_SIZE[1]/2, INPUT_NAMES_WIDTH, 40]]

NAMES_WIDTH = 300
SHOW_NAMES = [[SQUARE_X - MARGIN*5 - NAMES_WIDTH, SQUARE_Y, NAMES_WIDTH, 44],
              [SQUARE_X + SQUARE_WIDTH + MARGIN*5, SQUARE_Y, NAMES_WIDTH, 44]]

CAPTURE_0 = [(SHOW_NAMES[0][0] + NAMES_WIDTH // 2, SQUARE_Y),
             (SHOW_NAMES[1][0] - SHOW_NAMES[0][0] + NAMES_WIDTH // 2, SQUARE_HEIGHT)]

# Final scoreboard

FINAL = [BOARD_X, 0, BOARD_WIDTH, BOARD_HEIGHT]

# Game constants
NO_HAND = 7                     # No of cards in hand