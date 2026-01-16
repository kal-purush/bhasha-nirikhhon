# Pygame template 1.0

# Import the pygame and system modules
import pygame
import sys

# --- Constants --- #

SCREEN_WIDTH = 800
SCREEN_HEIGHT = 600

FPS = 60 # Frames per second

WHITE = (255, 255, 255) # RGB triplet saved in a tuple



# --- Initialize Pygame ---
pygame.init()

# --- Screen setup --- #
screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
pygame.display.set_caption("Pygame Template")

# --- Clock setup --- #
clock = pygame.time.Clock() # Note the capital C in the word clock!

# --- Game loop --- #
running = True
while running:
# --- Listen for and handle events --- #
    for event in pygame.event.get():
        if event.type == pygame.QUIT: # Type the QUIT event in UPPERCASE!
            running = False

 # --- Game logic --- #
 # (This is where you'll put your game's logic)

 # --- Draw --- #
    screen.fill(WHITE) # Fill screen background with white

 # (This is where you'll draw your game objects)

    pygame.display.flip() # Update the display

 # --- Limit frames per second (FPS) --- #

    clock.tick(FPS)

 # --- Quit Pygame and exit system module --- #
pygame.quit()
sys.exit()