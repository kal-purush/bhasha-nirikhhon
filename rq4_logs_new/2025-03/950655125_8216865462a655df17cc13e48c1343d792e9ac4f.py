import pygame
from pygame.draw import *

pygame.init()

FPS = 30
screen = pygame.display.set_mode((900, 900))
screen.fill((255, 255, 255))

circle(screen, (255, 255, 0), (200, 200), 150)  # круг желтый (тело)
circle(screen, (0, 0, 0), (200, 200), 150, 1)  # обводка
rect(screen, (0, 0, 0), (120, 270, 155, 20))  # прямоугольник (рот)
circle(screen, (255, 0, 0), (130, 150), 30)  # круг красный большой
circle(screen, (0, 0, 0), (130, 150), 30, 1)  # обводка
circle(screen, (0, 0, 0), (130, 150), 10)  # круг черный (зрачок левый)
circle(screen, (255, 0, 0), (260, 150), 20)  # круг красный меленький
circle(screen, (0, 0, 0), (260, 150), 20, 1)  # обводка
circle(screen, (0, 0, 0), (260, 150), 10)  # круг черный (зрачок правый)
polygon(screen, (0, 0, 0), [(70, 50), (85, 35), (185, 135), (170, 150)])
polygon(screen, (0, 0, 0), [(210, 135), (225, 150), (365, 75), (350, 60)])

pygame.display.update()
clock = pygame.time.Clock()
finished = False

while not finished:
    clock.tick(FPS)
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            finished = True

pygame.quit()