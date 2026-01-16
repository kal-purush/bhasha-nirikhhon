#import custom_maze

import pygame
from pygame.locals import *
from custom_maze import maze

BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
PURPLE = (138, 43, 226)
GREEN = (0, 255, 0)
RED = (255, 0, 0)
BLUE = (0, 0, 255)

WIDTH = 40
HEIGHT = 40

MARGIN = 5
'''
grid = []
# Loop for each row
for row in range(4):
    # For each row, create a list that will
    # represent an entire row
    grid.append([])
    # Loop for each column
    for column in range(5):
        # Add a the number zero to the current row
        grid[row].append(0)

grid[0][1] = 1
grid[1][1] = 1
grid[0][3] = 1
grid[1][3] = 1
grid[2][2] = 1
grid[2][3] = 1
grid[3][0] = 1
grid[3][4] = 2
grid[0][0] = 3
'''
grid = maze
n = len(grid)
m = len(grid[0])
robot_pos = (3,4)


pygame.init()

WINDOW_SIZE = [300, 300]
screen = pygame.display.set_mode(WINDOW_SIZE)
 
done = False

clock = pygame.time.Clock()
 
# -------- Main Program Loop -----------
while not done:
    for event in pygame.event.get():  # User did something
        if event.type == pygame.QUIT:  # If user clicked close
            done = True  # Flag that we are done so we exit this loop
        elif event.type == pygame.MOUSEBUTTONDOWN:
            # User clicks the mouse. Get the position
            pos = pygame.mouse.get_pos()
            # Change the x/y screen coordinates to grid coordinates
            column = pos[0] // (WIDTH + MARGIN)
            row = pos[1] // (HEIGHT + MARGIN)
            # Set that location to one
            grid[row][column] = 1
            print("Click ", pos, "Grid coordinates: ", row, column)
        elif event.type == pygame.KEYDOWN:
            if event.key == K_DOWN:
                if robot_pos[0]+1 < n and maze[robot_pos[0]+1][robot_pos[1]]!=1:
                    robot_pos = (robot_pos[0]+1,robot_pos[1])
            if event.key == K_UP:
                if robot_pos[0]-1 >= 0 and maze[robot_pos[0]-1][robot_pos[1]]!=1:
                    robot_pos = (robot_pos[0]-1,robot_pos[1])
            if event.key == K_LEFT:
                if robot_pos[1]-1 >= 0 and maze[robot_pos[0]][robot_pos[1]-1]!=1:
                    robot_pos = (robot_pos[0],robot_pos[1]-1)
            if event.key == K_RIGHT:
                if robot_pos[1]+1 < m and maze[robot_pos[0]][robot_pos[1]+1]!=1:
                    robot_pos = (robot_pos[0],robot_pos[1]+1)
                #pos = pygame.mouse.get_pos()
                #row = pos[1]
                # grid[row][column] = 5
                #print("Click ", pos, "Grid coordinates: ", row, column)
 
    # Set the screen background
    screen.fill(PURPLE)
 
    # Draw the grid
    for row in range(n):
        for column in range(m):
            color = WHITE
            if grid[row][column] == 1:
                color = BLACK
            elif row == robot_pos[0] and column == robot_pos[1]:
                color = BLUE
            elif grid[row][column] == 2:
                color = GREEN
            elif grid[row][column] == 3:
                color = RED
            
            pygame.draw.rect(screen,
                             color,
                             [(MARGIN + WIDTH) * column + MARGIN,
                              (MARGIN + HEIGHT) * row + MARGIN,
                              WIDTH,
                              HEIGHT])
 
    # Limit to 60 frames per second
    clock.tick(60)
 
    # Go ahead and update the screen with what we've drawn.
    pygame.display.flip()
 
# Be IDLE friendly. If you forget this line, the program will 'hang'
# on exit.
pygame.quit()