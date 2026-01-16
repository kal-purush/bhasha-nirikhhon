__author__ = 'William'
import pygame
import time
import random
#initializer for pygame
pygame.init()
pygame.mixer.init(44100, -16, 2, 2048)

#colors and font
white = (255, 255, 255)
black = (0, 0, 0)
green = (0, 155, 0)
red = (255, 0, 0)
smallfont = pygame.font.SysFont("comicsansms", 25)
medfont = pygame.font.SysFont("comicsansms", 50)
largefont = pygame.font.SysFont("comicsansms", 80)

#define finals area
display_width = 1200
display_height = 800
block_size = 20
FPS = 30
appleThickness = 30
snake_direction = "right"
#python gameDisplay
gameDisplay = pygame.display.set_mode((display_width, display_height))
pygame.display.set_caption('Snakey Snakey Game')
icon =  pygame.image.load('apple.png')
pygame.display.set_icon(icon)
#images!
SnakeHeadImg = pygame.image.load('snakehead.png')
AppleImg = pygame.image.load('apple.png')
#sound!
mlg_sound = pygame.mixer.Sound(b'applause_y.wav')
#fps
clock = pygame.time.Clock()

def score(score):
    text = smallfont.render("Score: " + str(score), True, black)
    gameDisplay.blit(text, [0, 0])


def rand_Appel_Gen():
    randAppleX = round(random.randrange(0, display_width- appleThickness))#,10)
    randAppleY = round(random.randrange(0, display_height-appleThickness))#,10)

    return randAppleX, randAppleY

def pause():
    paused = True
    message_to_screen("Paused", black, -100, "large")
    message_to_screen("Press C to continue or Q to quit", black, 25)
    pygame.display.update()
    while paused:
        for event  in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                quit()
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_c:
                    paused = False

                elif event.key == pygame.K_q:
                    pygame.quit()
                    quit()
        #gameDisplay.fill(white)
        clock.tick(5)


def game_Menu():
    menu = True
    while menu:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                quit()
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_RETURN:
                    menu = False
                if event.key == pygame.K_q:
                    pygame.quit()
                    quit()

        gameDisplay.fill(white)
        message_to_screen("Welcome to Snakey Game", green, -100, "large")
        message_to_screen("Objective : Eat them red apples!", red, -30)
        message_to_screen("Eat more apples to get longer!", black, 10)
        message_to_screen("Dont run into your self or the egdes, you'll die!", black, 50)
        message_to_screen("Press Enter to play,P to pause or Q to quit!", black, 180)

        pygame.display.update()
        clock.tick(15)


#Snake function to handle enlargement and drawing!
def snake(block_size,snakeList):
    if snake_direction == "right":
        head = pygame.transform.rotate(SnakeHeadImg,270)
    if snake_direction == "left":
        head = pygame.transform.rotate(SnakeHeadImg,90)
    if snake_direction == "up":
        head = SnakeHeadImg
    if snake_direction == "down":
        head = pygame.transform.rotate(SnakeHeadImg,180)

    gameDisplay.blit(head, (snakeList[-1][0],snakeList[-1][1]))
    for XnY in snakeList[:-1]:
         #Draw that snake                    #PossitionX,PossitionY ,WIDTH , HEIGHT
        pygame.draw.rect(gameDisplay,green,[XnY[0], XnY[1], block_size, block_size])

def text_objects(text, color, size):
    if size == "small":
      textSurFace =  smallfont.render(text, True, color)
    elif size == "medium":
      textSurFace = medfont.render(text, True, color)
    elif size == "large":
      textSurFace = largefont.render(text, True, color)
    return textSurFace, textSurFace.get_rect()

def message_to_screen(msg, color, y_displace=0, size = "small"):
    #screen_text = font.render(msg, True, color)
    #gameDisplay.blit(screen_text, [display_width/2, display_height/2])
    textSurf ,textRect = text_objects(msg,color,size)
    textRect.center = (display_width/2), (display_height/2)+y_displace
    gameDisplay.blit(textSurf, textRect)


#MAIN GAME LOOP
def gameLoop():
    global snake_direction
    snake_direction = "right"
    not_permited_direction = ""
    gameExit = False
    gameOver = False

    snakeList = []
    snakeLength = 1

    lead_x = display_width/2
    lead_y = display_height/2
    lead_x_change = 0
    lead_y_change = 0
    randAppleX, randAppleY = rand_Appel_Gen()

    while not gameExit:
        if gameOver == True:
            message_to_screen("Game over", red, -50, "large")
            message_to_screen("Press Enter to play again or Q to quit", black, 50, "medium")
            pygame.display.update()
        while gameOver == True:
            #gameDisplay.fill(white)


            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    gameExit = True
                    gameOver = False
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_q:
                        gameExit = True
                        gameOver = False
                    if event.key == pygame.K_RETURN:
                        #recusrivity... kinda bad might want to change that up
                        gameLoop()



        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                gameExit = True
            if event.type == pygame.KEYDOWN:
                if not_permited_direction != snake_direction:
                    if event.key == pygame.K_LEFT:
                        snake_direction = "left"
                        not_permited_direction = "right"
                        lead_x_change = -block_size
                        lead_y_change = 0
                    elif event.key == pygame.K_RIGHT:
                        snake_direction = "right"
                        not_permited_direction = "left"
                        lead_x_change = block_size
                        lead_y_change = 0
                    elif event.key == pygame.K_UP:
                        snake_direction = "up"
                        not_permited_direction = "down"
                        lead_y_change = -block_size
                        lead_x_change = 0
                    elif event.key == pygame.K_DOWN:
                        snake_direction = "down"
                        not_permited_direction = "up"
                        lead_y_change = block_size
                        lead_x_change = 0
                    elif event.key == pygame.K_p:
                            pause()
            """ stop the movement if needs be someday
            if event.type == pygame.KEYUP:
                if event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT:
                    lead_x_change = 0
                if event.key == pygame.K_UP or event.key == pygame.K_DOWN:
                    lead_y_change = 0
                    """
        #Game logic!

        #Border bounderies
        if lead_x >= display_width or lead_x < 0 or lead_y >= display_height or lead_y < 0:
            gameOver = True


        lead_x += lead_x_change
        lead_y += lead_y_change
        gameDisplay.fill(white)
        #draw that apple
        #pygame.draw.rect(gameDisplay, red, [randAppleX, randAppleY, appleThickness, appleThickness])
        gameDisplay.blit(AppleImg, (randAppleX, randAppleY))

        snakeHead = []
        snakeHead.append(lead_x)
        snakeHead.append(lead_y)
        snakeList.append(snakeHead)

        if len(snakeList) > snakeLength:
            del snakeList[0]
                        #check until the last element!
        for eachSegement in snakeList[:-1]:
            if eachSegement == snakeHead:
                gameOver = True


        #Draw that snake doe!
        snake(block_size, snakeList)

        #update the score
        score(snakeLength-1)
        if snakeLength == 2:
            pygame.mixer.Sound.play(mlg_sound)
        #better way to draw rects
            #gameDisplay.fill(red,rect=[200, 200, 50, 50])
        pygame.display.update()

        #OM om om of the apple!
       ## if lead_x == randAppleX and lead_y == randAppleY:
       ##     randAppleX = random.randrange(0, display_width- block_size,10)
       ##     randAppleY = random.randrange(0, display_height-block_size,10)
       ##     snakeLength+=1
        # if lead_x >= randAppleX and lead_x <= randAppleX +appleThickness:
        #      if lead_y >= randAppleY and lead_y <= randAppleY +appleThickness:
        #             randAppleX = round(random.randrange(0, display_width- appleThickness))#,10)
        #             randAppleY = round(random.randrange(0, display_height-appleThickness))#,10)
        #             snakeLength += 1
        if lead_x > randAppleX and lead_x < randAppleX + appleThickness or lead_x + block_size > randAppleX and lead_x +block_size < randAppleX + appleThickness:
                if lead_y > randAppleY and lead_y < randAppleY + appleThickness or lead_y + block_size > randAppleY and lead_y +block_size < randAppleY + appleThickness:
                    randAppleX, randAppleY = rand_Appel_Gen()
                    snakeLength += 1

        clock.tick(FPS)

    pygame.quit()
    quit()
game_Menu()
gameLoop()

