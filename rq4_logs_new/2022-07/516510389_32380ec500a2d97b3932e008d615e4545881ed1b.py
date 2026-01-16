import pygame
import os
import time

FPS= 60
WHITE=(255, 255, 255)
BLACK=(0, 0, 0,)
WIDTH, HEIGHT=900, 500
WIN=pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption('online race')
border=pygame.Rect(880, 0, 10, 500)
road=pygame.image.load(os.path.join('Asset','road.png'))

#importing player image
cyan_image=pygame.image.load(os.path.join('Asset','cyan.png'))
pink_image=pygame.image.load(os.path.join('Asset','pink.png'))
red_image=pygame.image.load(os.path.join('Asset','red.png'))
yellow_image=pygame.image.load(os.path.join('Asset','yellow.png'))



def draw_window(cyan, pink, red, yellow):
    WIN.blit(road,(0, 0))
    #WIN.fill(WHITE)
    WIN.blit(cyan_image,(cyan.x, cyan.y))
    WIN.blit(pink_image,(pink.x, pink.y))
    WIN.blit(red_image,(red.x, red.y))
    WIN.blit(yellow_image,(yellow.x, yellow.y))
    pygame.draw.rect(WIN, BLACK, border)


    if cyan.x>=800:
        pygame.display.set_caption(' cyan just won the game')
        time.sleep(3)
        quit()
    if pink.x>=800:
        pygame.display.set_caption('pink just won the game')
        time.sleep(3)
        quit()
    if red.x>=800:
        pygame.display.set_caption('red just won the game')
        time.sleep(3)
        quit()
    if yellow.x>=800:
        pygame.display.set_caption('yellow just won the game')
        time.sleep(3)
        quit()



    pygame.display.update()

def main():
    cyan=pygame.Rect(22, 25 , 100, 100)
    pink=pygame.Rect(22, 150 , 100, 100)
    red=pygame.Rect(22, 270 , 100, 100)
    yellow=pygame.Rect(22, 390 , 100, 100)


    clock=pygame.time.Clock()
    run= True
    while run: 
        clock.tick(FPS)
        for event in pygame.event.get():
            if event.type==pygame.QUIT:
                run=False
            if event.type==pygame.KEYDOWN:
                if event.key==pygame.K_1:
                    cyan.x +=8
                if event.key==pygame.K_2:
                    pink.x +=8
                if event.key==pygame.K_3:
                    red.x +=8
                if event.key==pygame.K_4:
                    yellow.x +=8
        draw_window(cyan, pink, red, yellow)

    pygame.quit()

main()