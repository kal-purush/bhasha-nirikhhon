#imports pygame modules
import pygame

#initializes pygame
pygame.init()

#sets my screen width and height variables
SCREEN_WIDTH = 800
SCREEN_HEIGHT = 600

player = pygame.Rect((300, 250, 50, 50))

#creates my screen and defines size
screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))

#variable that sets the background color to black


#sets the name of the window
pygame.display.set_caption('Tutoring with Joe')

run = True

while run == True:

	screen.fill((0, 0, 0))

	pygame.draw.rect(screen, (255, 0, 0), player)

	key = pygame.key.get_pressed()
	if key[pygame.K_a] == True:
		player.move_ip(-1, 0)
	elif key[pygame.K_d] == True:
		player.move_ip(1, 0)
	elif key[pygame.K_w] == True:
		player.move_ip(0, -1)
	elif key[pygame.K_s] == True:
		player.move_ip(0, 1)

	for event in pygame.event.get():
		if event.type == pygame.QUIT:
			run = False

	pygame.display.update()

pygame.quit()