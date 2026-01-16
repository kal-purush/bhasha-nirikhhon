#создай игру "Лабиринт"!
from pygame import *
windows = display.set_mode((700,500))
background =  transform.scale(image.load("004.jpg"),(700,500))

display.set_caption('pin-pong')
class gamesprite(sprite.Sprite):
    def __init__(self,image1,speed,x,y,):
        super().__init__()
        self.image = transform.scale(image.load(image1),(65,65))
        self.speed = speed
        self.rect = self.image.get_rect()
        self.rect.y = y
        self.rect.x = x
        
    def reset(self):
        windows.blit(self.image,(self.rect.x,self.rect.y))

##########################################################
class Player1 (gamesprite):
    def update(self):
        keys_pressed = key.get_pressed()
        if keys_pressed[K_w] and self.rect.y > 1:
            self.rect.y -= self.speed
        if keys_pressed[K_s] and self.rect.y < 438:
            self.rect.y += self.speed
###########################################################
class Player2 (gamesprite):
    
    def update(self):
        keys_pressed = key.get_pressed()
        if keys_pressed[K_UP] and self.rect.y > 1:
            self.rect.y -= self.speed
        if keys_pressed[K_DOWN] and self.rect.y < 438:
            self.rect.y += self.speed
 ###########################################################   
class Ball (gamesprite):
    speed_x = 3
    speed_y = 3
    def update(self):
        self.rect.x += self.speed_x
        self.rect.y += self.speed_y
        if self.rect.y < 0 or self.rect.y > 410:
            self.speed_y *= -1
        if sprite.collide_rect(self, hero) or sprite.collide_rect(self, hero1):
            self.speed_x *= -1

#class wlan(sprite.Sprite):
#    def __init__(self,color_1,color_2,color_3,wall_x,wall_y,wall_width,wall_height):
#        super().__init__()
#        self.color_1 = color_1
#        self.color_2 = color_2
#        self.color_3 = color_3
#        self.width = wall_width
#        self.height = wall_height
#        self.image = Surface((self.width, self.height))
#        self.image.fill(color_1,color_2,color_3)
#        self.rect = self.image.get_rect()
#        
#        self.rect.y = wall_y
#    def draw_wall(self):
#        windows.blit(self.image,(self.rect.y,self.rect.x))

        



hero1 =Player2('001.jpg',1,635,0)
hero = Player1('002.jpg',1,0,0)
hero2 =gamesprite('download.jpg',5,400,400)
game = True

while game:

    windows.blit(background,(0,0))
    hero.update()
    hero1.update()
    hero.reset()
    hero1.reset()
    hero2.reset()
    for e in event.get():
        if e.type == QUIT:
            game = False
    if e != True:
        windows.blit(background,(0,0))
        hero.update()
        hero1.update()
        hero.reset()
        hero1.reset()
        hero2.update()
        hero2.reset()
    
        display.update()