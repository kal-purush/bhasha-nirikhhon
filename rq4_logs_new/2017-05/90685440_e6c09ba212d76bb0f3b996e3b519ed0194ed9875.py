import pygame
from random import randint

pygame.init()
clock = pygame.time.Clock()
FPS = 15

white = (255, 255, 255)
red = (255, 0, 0)
green = (0, 255, 0)
blue = (0, 0, 255)
black = (0, 0, 0)
gray = (244, 244, 244)

display_width = 600
display_height = 400

gameDisplay = pygame.display.set_mode((display_width, display_height))
pygame.display.set_caption("Basalisk")

gameExit = False

lead_x = display_width/2
lead_y = display_height/2

lead_x_change = 0
lead_y_change = -10

appleExists = False
apple_x = 0
apple_y = 0
is_blue = 1

snake_parts = [[lead_x,lead_y]]

turned = False

while not gameExit:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            gameExit = True
        if event.type == pygame.KEYDOWN and not turned:
            if event.key == pygame.K_LEFT and lead_x_change == 0:
                lead_x_change = -10
                lead_y_change = 0
                turned = True
            elif event.key == pygame.K_RIGHT and lead_x_change == 0:
                lead_x_change = 10
                lead_y_change = 0
                turned = True
            elif event.key == pygame.K_UP and lead_y_change == 0:
                lead_y_change = -10
                lead_x_change = 0
                turned = True
            elif event.key == pygame.K_DOWN and lead_y_change == 0:
                lead_y_change = 10
                lead_x_change = 0
                turned = True

    if not (0 <= lead_x <= display_width - 10) or not (0 <= lead_y <= display_height - 10):
        gameExit = True

    if (lead_x == apple_x) and (lead_y == apple_y):
        if (is_blue == 0):
            snake_parts += [[lead_x,lead_y],[lead_x,lead_y],[lead_x,lead_y]]
        else:
            snake_parts += [[lead_x,lead_y]]
        appleExists = False

    i = len(snake_parts) - 1
    while (i > 0):
        snake_parts[i][0] = snake_parts[i-1][0]
        snake_parts[i][1] = snake_parts[i-1][1]
        i -= 1

    lead_x += lead_x_change
    lead_y += lead_y_change

    for part in snake_parts:
        if (part[0] == lead_x) and (part[1] == lead_y):
            gameExit = True

    snake_parts[0] = [lead_x, lead_y]

    while not (appleExists):
        is_blue = randint(0,9)
        apple_x = randint(0, ((display_width - 10)/10)) * 10
        apple_y = randint(0, ((display_height - 10)/10)) * 10
        appleExists = True
        for part in snake_parts:
            if (apple_x == part[0]) and (apple_y == part[1]):
                appleExists = False


    gameDisplay.fill(gray)
    if (is_blue == 0):
        pygame.draw.rect(gameDisplay, blue, [apple_x, apple_y, 10, 10])
    else:
        pygame.draw.rect(gameDisplay, red, [apple_x, apple_y, 10, 10])

    x = 0
    while (x < len(snake_parts)):
        pygame.draw.rect(gameDisplay, green, [snake_parts[x][0], snake_parts[x][1], 10, 10])
        x+=1

    if (lead_y_change == -10):
        pygame.draw.rect(gameDisplay, black, [lead_x + 1, lead_y + 1, 2, 2])
        pygame.draw.rect(gameDisplay, black, [lead_x + 7, lead_y + 1, 2, 2])
    elif (lead_y_change == 10):
        pygame.draw.rect(gameDisplay, black, [lead_x + 1, lead_y + 6, 2, 2])
        pygame.draw.rect(gameDisplay, black, [lead_x + 7, lead_y + 6, 2, 2])
    elif (lead_x_change == 10):
        pygame.draw.rect(gameDisplay, black, [lead_x + 6, lead_y + 1, 2, 2])
        pygame.draw.rect(gameDisplay, black, [lead_x + 6, lead_y + 7, 2, 2])
    elif (lead_x_change == -10):
        pygame.draw.rect(gameDisplay, black, [lead_x + 1, lead_y + 1, 2, 2])
        pygame.draw.rect(gameDisplay, black, [lead_x + 1, lead_y + 7, 2, 2])

    pygame.display.update()
    turned = False

    clock.tick(FPS)

pygame.quit()
print "score: "
print len(snake_parts) * 10 - 10
quit()