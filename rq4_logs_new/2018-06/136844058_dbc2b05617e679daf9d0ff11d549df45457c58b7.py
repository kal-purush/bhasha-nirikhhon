import pygame
import sys

from CONSTANTS import *

class Object_2D(pygame.sprite.Sprite):

    def __init__(self, spawnPoint, size, color):
        pygame.sprite.Sprite.__init__(self)
        self.image = pygame.Surface(size)
        self.image.fill(color)
        self.rect = self.image.get_rect()
        self.rect.x = spawnPoint[0]
        self.rect.y = spawnPoint[1]
        self.spawnPoint = spawnPoint
        
class Bullet(Object_2D):
    
    def __init__(self, spawnPoint, size, color):
        Object_2D.__init__(self, spawnPoint, size, color)
        self.speed = 45

    def update(self):
        self.rect.x += self.speed
        if self.rect.x > SCREEN_WIDTH:
            self.kill()

class Enemy(Object_2D):

    def __init__(self, spawnPoint, size, color):
        Object_2D.__init__(self, spawnPoint, size, color)

    def update(self):
        self.rect.x -= 1
        if self.rect.x < 0:
            self.kill()

class Player(Object_2D):

    def __init__(self, spawnPoint, size, color):
        Object_2D.__init__(self, spawnPoint, size, color)
        self.speed = 5
        self.x_direction = 0
        self.y_direction = 0

    def update(self):
        self.rect.x += self.speed * self.x_direction
        self.rect.y += self.speed * self.y_direction