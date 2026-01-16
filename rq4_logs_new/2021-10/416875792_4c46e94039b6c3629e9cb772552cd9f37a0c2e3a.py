import pygame
from random import randint
BLACK = (0,0,0)
 
class Ball(pygame.sprite.Sprite):
    RADIUS = 10

    def __init__(self,x,y,vx,vy,screen,fgcolor,bgcolor):
        self.x = x
        self.y = y
        self.screen = screen
        self.vx = vx
        self.vy = vy
        self.fgcolor = fgcolor
        self.bgcolor = bgcolor
    
    def update(self):
        w, h = self.screen.get_size()
        if self.x <= (20 + (self.RADIUS +1)) or self.x >= w :
            self.vx = -self.vx 
        if self.y <= (20 + (self.RADIUS)) or self.y >= (h-(20 + (self.RADIUS))):
            self.vy = -self.vy 
        self.show(self.bgcolor)
        self.x = self.x + self.vx
        self.y = self.y + self.vy
        self.show(self.fgcolor)

        


    def show(self,color):
        pygame.draw.circle(self.screen,color,(self.x,self.y),self.RADIUS)










def __init__(self, color, width, height):
    #super().__init__()
        
    # Set the background color and set it to be transparent
    self.image = pygame.Surface([width, height])
    self.image.fill(BLACK)
    self.image.set_colorkey(BLACK)

    # Draw the ball 
    pygame.draw.rect(self.image, color, [0, 0, width, height])
    
    self.velocity = [randint(4,8),randint(-8,8)]
    
    self.rect = self.image.get_rect()

def update(self):
    self.rect.x += self.velocity[0]
    self.rect.y += self.velocity[1]

#