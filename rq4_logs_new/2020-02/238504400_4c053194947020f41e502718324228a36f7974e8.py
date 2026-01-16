import pygame
import random
from pygame import Color
successes, failures = pygame.init()
print("Initializing pygame: {0} successes and {1} failures.".format(successes, failures))

h=480
w=720

screen = pygame.display.set_mode((720, 480))
clock = pygame.time.Clock()
FPS = 60

BLACK = Color(0, 0, 0)
WHITE = Color(255, 255, 255)
GREEN = Color(0, 255, 0) 
BLUE = Color(0, 0, 128)
RED = Color(255,0,0) 

speed=200
sprites=[]
running = True
gameover=False
pause=False
player=None
killedfoe=0

GAME_FONT = pygame.font.Font("font.ttf", 24)

class Player(pygame.sprite.Sprite):
    def __init__(self):
        super().__init__()
        self.w=50
        self.h=50
        # self.image = pygame.Surface((self.w, self.h))
        image = pygame.image.load("player.png").convert()
        self.image=pygame.transform.scale(image, (self.w, self.h))
        # self.image.fill(WHITE)
        self.rect = self.image.get_rect()  # Get rect of some size as 'image'.
        self.velocity = [0,0]
        self.rect.topleft=[w/2-self.w,h-self.h]
        self.role="player"
        print(".\n")
    def update(self):
        x,y=player.rect.topleft
        gox,goy=self.velocity
        if(x+gox<=0):
            x=0
            gox=0
            self.rect.topleft=[0,y]
        if(x+gox>=w-self.w):
            x=w-self.w
            gox=0
            self.rect.topleft=[w-self.w,y]
        if(y+goy<=0):
            y=0
            goy=0
            self.rect.topleft=[x,y]
        if(y+goy>=h-self.h):
            y=h-self.h
            goy=0
        self.velocity=[gox,goy]
        self.rect.move_ip(self.velocity)
    def colliderect(self):
        for sprite in sprites:
            if sprite!=self:
                if sprite.role=="foe":
                    if sprite.rect.colliderect(self):
                        global gameover
                        gameover=True
                        

class Fire(pygame.sprite.Sprite):
    def __init__(self):
        super().__init__()
        self.role="fire"
        self.w=10
        self.h=20
        # self.image = pygame.Surface((self.w, self.h))
        image = pygame.image.load("fire.png").convert()
        self.image=pygame.transform.scale(image, (self.w, self.h))
        # self.image.fill(WHITE)
        self.rect = self.image.get_rect()  # Get rect of some size as 'image'.
    def update(self):
        x,y=self.rect.topleft
        y-=10
        self.rect.topleft=[x,y]
    def colliderect(self):
        for sprite in sprites:
            if sprite!=self:
                if sprite.role=="foe":
                    if sprite.rect.colliderect(self):
                        sprites.remove(self)
                        sprites.remove(sprite)
                        global killedfoe
                        killedfoe+=1


class Foe(pygame.sprite.Sprite):
    def __init__(self):
        super().__init__()
        self.role="foe"

        self.w=random.randint(50,100)
        self.h=self.w
        image = pygame.image.load("v.png").convert()
        self.image=pygame.transform.scale(image, (self.w, self.h))
        self.rect = self.image.get_rect()  # Get rect of some size as 'image'.
        x=random.randint(0,w-self.w)
        self.rect.topleft=[x,0-self.h]
    def update(self):
        x,y=self.rect.topleft
        y+=2
        self.rect.topleft=[x,y]
    def colliderect(self):
        if self.rect.topleft[1]>h+self.h:
            sprites.remove(self)


first=True


def init():
    global sprites
    global player
    global running
    global gameover
    global pause
    global first
    global killedfoe

    sprites=[]
    running = True
    gameover=False
    killedfoe=0
    pause=False
    player = Player()
    sprites.append(player)
    first=False


while running:
    dt = clock.tick(FPS) / 1000  # Returns milliseconds between each call to 'tick'. The convert time to seconds.
    screen.fill(BLACK)  # Fill the screen with background color.


    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if pause==False and gameover==False:
                if event.key == pygame.K_w:
                    player.velocity[1] = -1 * speed * dt  # 200 pixels per second
                    player.velocity[0] = 0
                if event.key == pygame.K_s:
                    player.velocity[1] = speed * dt
                    player.velocity[0] = 0
                if event.key == pygame.K_a:
                    player.velocity[0] = -1 * speed * dt
                    player.velocity[1] = 0
                if event.key == pygame.K_d:
                    player.velocity[0] = speed * dt
                    player.velocity[1] = 0
                if event.key == pygame.K_j:
                    fire = Fire()
                    player_x,player_y=player.rect.topleft
                    fire_x=player_x+player.w/2-fire.w/2
                    fire_y=player_y
                    fire.rect.topleft=(fire_x,fire_y)
                    sprites.append(fire)

            if event.key == pygame.K_p:
                if pause:
                    pause=False
                else:
                    pause=True

            if event.key == pygame.K_r and gameover:
                init()

            if event.key == pygame.K_b and first:
                init()

        # pygame.time.delay(4500)
    if first:
        text = GAME_FONT.render("press b to play", True, RED,BLACK) 
        textRect = text.get_rect()
        textRect.center = (w // 2, h // 2) 
        screen.blit(text, textRect) 
    else:
        for sprite in sprites:
            screen.blit(sprite.image, sprite.rect)
            text = GAME_FONT.render("killed:{}".format(killedfoe), True, RED,BLACK) 
            textRect = text.get_rect()
            # textRect.center = (w // 2, h // 2) 
            textRect.topleft=[10,10]
            screen.blit(text, textRect) 

            if gameover:
                print("gameover")
                text = GAME_FONT.render("GAME OVER", True, RED,BLACK) 
                textRect = text.get_rect()
                textRect.center = (w // 2, h // 2) 
                screen.blit(text, textRect) 

                text = GAME_FONT.render("press r to restart", True, RED,BLACK) 
                textRect = text.get_rect()
                textRect.center = (w // 2+30, h // 2) 
                screen.blit(text, textRect) 
            elif pause:
                text = GAME_FONT.render("Pause", True, RED,BLACK) 
                textRect = text.get_rect()
                textRect.center = (w // 2, h // 2) 
                screen.blit(text, textRect) 
            else:
                sprite.update()
                sprite.colliderect()
                x=random.randint(0,300)
                if(x==9):
                    sprites.append(Foe())
    

    pygame.display.update()

print("Exited the game loop. Game will quit...")
quit()