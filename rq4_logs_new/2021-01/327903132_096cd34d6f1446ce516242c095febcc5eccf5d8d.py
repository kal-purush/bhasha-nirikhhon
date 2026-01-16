import pygame
from pygame.locals import *
from draw import *

class Button(pygame.sprite.Sprite):
    def __init__(self, x, y, color=BLACK):
        super().__init__() 
        w, h = 50, 50
        self.image = pygame.Surface((w, h))
        self.image.fill(color)
        self.rect = self.image.get_rect(center = (x, y))
        self.color = color

    def update(self):
        global current_color
        current_color = self.color
        print('button clicked', current_color)