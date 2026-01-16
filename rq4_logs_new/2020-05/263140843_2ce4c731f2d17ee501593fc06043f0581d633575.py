#bounce logic for atary.py
import time
import random
import pygame
pygame.init()

window_width = 600
window_height = 600
window = pygame.display.set_mode((window_width, window_height))
pygame.display.set_caption('Bouncing...')

clock = pygame.time.Clock()
yellow = (220, 255, 10)
black = (0, 0, 0)
ballx = 0
bally = 500
ballxchange = 0
ballychange = 0
oldballx = 0
oldbally = 0
speed = 50
nevertrue = False
game_over = False


ballxchange = random.randint(1, 10)
ballychange = random.randint(-10, -1)
print(ballxchange, ballychange)

while not game_over:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            game_over = True
        elif event.type == pygame.KEYDOWN:
            print("key down")
            if event.key == pygame.K_SPACE:
                ballxchange = 0
                ballychange = 0

    window.fill(black)
    oldballx = ballx
    oldbally = bally

    ballx += ballxchange
    bally += ballychange


    if bally <= 10 or bally >= window_height-10:
        ballychange = -ballychange

        if nevertrue == True:
            num = random.randint(1,2)
            if num == 1:
                ballychange = -ballychange
            else:
                ballychange = int((-ballychange) * random.uniform(0.6, 3))
            if ballychange > 15:
                ballychange = int(ballychange/3)

    elif ballx <= 0 or ballx >= window_width-10:
        ballxchange = -ballxchange

        if nevertrue == True:
            num = random.randint(1,2)
            if num == 1:
                ballxchange = -ballxchange
            else:
                ballxchange = int((-ballxchange) * random.uniform(0.6, 3))
            if ballxchange > 15:
                ballxchange = int(ballxchange/3)

    pygame.draw.circle(window, yellow, (ballx, bally), 10)
    pygame.display.update()
    clock.tick(speed)