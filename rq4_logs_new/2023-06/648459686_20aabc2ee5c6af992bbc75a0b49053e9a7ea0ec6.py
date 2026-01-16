# This is a simple snake game with the snake eating the food
# @author: Anirudh Asuri Mukundan
# @begin date: 1st June 2023

# Game objective: snake eating the food
# Game rules:
# 1. Hitting the corner ends the game
# 2. Eating the food increases snake's length
# 3. Crashing onto itself ends the game

import numpy as np
import matplotlib.pyplot as plt
import random
import curses # used to initialize the screen

s = curses.initscr()
curses.curs_set(0)
sh, sw = s.getmaxyx() # get the screen height and width
w = curses.newwin(sh, sw, 0, 0) # create a new window with the specified height and width and the origin placed at top left corner of the screen (0, 0)
w.keypad(1) # setting the program to accept keypad input
w.timeout(100) # refresh the screen every 100 milliseconds

# creating snake's initial position
snk_x = sw/4
snk_y = sh/2
## creating snake's body
snake = [ [snk_y, snk_x], [snk_y, snk_x-1], [snk_y, snk_x-2] ]

# create the food
food = [sh/2, sw/2] # food located at center of the screen
w.addch(food[0], food[1], curses.ACS_PI) # add the food to the screen using add character function

# initial direction of motion of snake
key = curses.KEY_RIGHT

# Track the movement of the snake
while True:
    next