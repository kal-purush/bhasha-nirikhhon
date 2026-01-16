import pygame
from pygame.locals import *
import time
import random
import sys

speed = 8

width = 700
height = 700

pygame.init()
screen = pygame.display.set_mode((width, height))
pygame.display.set_caption('SNAKE GAME')

black = (0, 0, 0)
red = (255, 0, 0)
blue = (0, 0, 255)
gray = (190, 188, 191)
green = (0, 255, 0)
white = (255, 255, 255)

snake_pos = [100, 100]
snake_body = []
for i in range(3):
    snake_body.append([100-(i*20), 100])

food_pos = [random.randrange(1, (width//20))*20, random.randrange(1, (height//20))*20]
food_spawn = True

direct = 'R'
new_direct = direct

score = 0

def game_over():
    game_over_font = pygame.font.SysFont('freesansbold.ttf', 84)
    game_over = game_over_font.render('GAME OVER !', True, red)
    game_over_rect = game_over.get_rect(center=(width//2 - 10, width//2 - 10))

    score_font = pygame.font.SysFont('consolas', 50)
    score_value = score_font.render('Total Score: ' + str(score), True, (green))
    score_rect = game_over.get_rect(center=(width//2, width//2 + 50))

    drawGrid(gray)
    s = pygame.Surface((700,700), pygame.SRCALPHA)
    s.fill((0,0,0,110))
    screen.blit(s, (0,0))

    screen.blit(game_over, game_over_rect)
    screen.blit(score_value, score_rect)
    pygame.display.flip()
    time.sleep(3)
    pygame.quit()
    sys.exit()


def show_score(x, y):
    score_font = pygame.font.SysFont('consolas', 25)
    score_value = score_font.render('Score: ' + str(score), True, (green))
    screen.blit(score_value, (x, y))

def drawGrid(color):
    x = 0
    y = 0
    w = width // 20
    for l in range(w):
        pygame.draw.aaline(screen, color, (0, y), (width, y))
        pygame.draw.aaline(screen, color, (x, 0), (x, width))
        x += 20
        y += 20
    pygame.display.update()

clock = pygame.time.Clock()
flag = True
while flag:

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            pygame.quit()
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_ESCAPE:
                pygame.quit()
            elif event.key == pygame.K_LEFT or event.key == ord('a'):
                new_direct = 'L'
            elif event.key == pygame.K_RIGHT or event.key == ord('d'):
                new_direct = 'R'
            elif event.key == pygame.K_UP or event.key == ord('w'):
                new_direct = 'U'
            elif event.key == pygame.K_DOWN or event.key == ord('s'):
                new_direct = 'D'

    if new_direct == 'U' and direct != 'D':
        direct = 'U'
    if new_direct == 'D' and direct != 'U':
        direct = 'D'
    if new_direct == 'L' and direct != 'R':
        direct = 'L'
    if new_direct == 'R' and direct != 'L':
        direct = 'R'

    if direct == 'U':
        snake_pos[1] -= 20
    elif direct == 'D':
        snake_pos[1] += 20
    elif direct == 'L':
        snake_pos[0] -= 20
    elif direct == 'R':
        snake_pos[0] += 20

    snake_body.insert(0, list(snake_pos))
    if snake_pos[0] == food_pos[0] and snake_pos[1] == food_pos[1]:
        score += 1
        food_spawn = False
    else:
        snake_body.pop()

    if not food_spawn:
        food_pos = [random.randrange(1, (width//20)) * 20, random.randrange(1, (width//20)) * 20]
    food_spawn = True

    screen.fill(black)
    for pos in snake_body:
        pygame.draw.rect(screen, green, (pos[0], pos[1], 20, 20))

    pygame.draw.rect(screen, red, (food_pos[0], food_pos[1], 20, 20))

    for block in snake_body[1:]:
        if snake_pos[0] == block[0] and snake_pos[1] == block[1]:
            flag = False
            game_over()
            
    if snake_pos[0] < 0 or snake_pos[0] > width-20:
        game_over()
    if snake_pos[1] < 0 or snake_pos[1] > width-20:
        flag = False
        game_over()

    show_score(10, 10)
    drawGrid(gray)
    pygame.display.update()
    clock.tick(speed)
        
    