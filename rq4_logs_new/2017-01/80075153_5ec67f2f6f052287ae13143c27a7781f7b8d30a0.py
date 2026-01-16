import pygame
from pygame.locals import *
import snake
from random import randint

pygame.init()

BLACK = (0,0,0)
WHITE = (255, 255, 255)
RED = (255, 0, 0)

size = [600, 600]
screen = pygame.display.set_mode(size)
sq = 20
pygame.display.set_caption("Snake")

clock = pygame.time.Clock()
done = False
s = snake.Snake()

food = (sq*(randint(0, size[0]/sq)), sq*randint(0, size[1]/sq))

while not done:
    # limit to 10 times per second. Otherwise keeps updating and moving. Uses up cpu power too. 
    clock.tick(10)

    for event in pygame.event.get():
        if event == QUIT:
            pygame.quit()
            sys.exit()

        if event.type == KEYDOWN:
            if event.key == K_LEFT:
                s.dx, s.dy = -1, 0
            if event.key == K_RIGHT:
                s.dx, s.dy = 1, 0
            if event.key == K_UP:
                s.dx, s.dy = 0, -1
            if event.key == K_DOWN:
                s.dx, s.dy = 0, 1

    screen.fill(BLACK)
    # draw snake
    for cell in s.body[-1::-1]:
        pygame.draw.rect(screen, WHITE, [cell[0]*sq, cell[1]*sq, 20, 20])
    # draw food    
    pygame.draw.rect(screen, RED, [food[0], food[1], 20, 20])

    s.move()
    pygame.display.update()
