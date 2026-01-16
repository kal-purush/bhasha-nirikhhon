import time
import pygame
import random

pygame.init()
screen_width=800
screen_height=600
screen=pygame.display.set_mode((screen_width,screen_height))
WHITE=(255,255,255)
BLACK=(0,0,0)
BLUE=(50,153,213)
RED=(213,50,0)
GREEN=(0,255,0)
YELLOW=(255,255,102)

snake_block=10
snake_speed=30


font_style=pygame.font.SysFont('Arial',50)

def message(msg,color):
    text=font_style.render(msg,True,color)
    screen.blit(text,(screen_width/2,screen_height/2))
def score_message(msg,color):
    text = font_style.render(msg, True, color)
    screen.blit(text, (40, 40))
clock=pygame.time.Clock()
pygame.display.set_caption("Snake")

def print_snake(snake_block,snake_list):
    for cords in snake_list:
        pygame.draw.rect(screen,BLACK,(cords[0],cords[1],snake_block,snake_block))

def gameloop():
    game_over = False
    snake_x = screen_width / 2
    snake_y = screen_height / 2

    snake_y_change = 0
    snake_x_change = 0

    snake_list=[]
    length_of_snake=1

    food_x=round(random.randrange(0,screen_width-snake_block)/10.0)*10.0
    food_y=round(random.randrange(0,screen_height-snake_block)/10.0)*10.0
    while not game_over:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                game_over = True
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_UP:
                    snake_y_change = -snake_block
                    snake_x_change = 0
                if event.key == pygame.K_DOWN:
                    snake_y_change = snake_block
                    snake_x_change = 0
                if event.key == pygame.K_LEFT:
                    snake_x_change = -snake_block
                    snake_y_change = 0
                if event.key == pygame.K_RIGHT:
                    snake_x_change = snake_block
                    snake_y_change = 0

        if snake_x >= screen_width or snake_x < 0 or snake_y >= screen_height or snake_y < 0:
            game_over = True
        snake_x += snake_x_change
        snake_y += snake_y_change
        screen.fill(WHITE)
        pygame.draw.rect(screen, BLUE, (food_x, food_y, snake_block, snake_block))
        snake_head=[snake_x,snake_y]
        snake_list.append(snake_head)
        if len(snake_list) > length_of_snake:
            del snake_list[0]
        print_snake(snake_block,snake_list)

        for coord in snake_list[:-1]:
            if (coord)==snake_head:
                game_over=True
        score_message(("Your score: "+str(length_of_snake)), BLACK)
        pygame.display.update()
        if snake_x==food_x and snake_y==food_y:
            food_x = round(random.randrange(0, screen_width - snake_block) / 10.0) * 10.0
            food_y = round(random.randrange(0, screen_height - snake_block) / 10.0) * 10.0
            length_of_snake+=1
        clock.tick(snake_speed)

    message("You lost", RED)
    pygame.display.update()
    time.sleep(2)

    pygame.quit()
    quit()
gameloop()