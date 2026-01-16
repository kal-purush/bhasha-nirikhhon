__author__ = 'Scott'
import pygame
from pytmx import *
from pytmx.util_pygame import load_pygame


class TileLayer(object):
    def __init__(self, index, mapObj):
        self.index = index
        self.tiles = pygame.sprite.Group()
        self.mapObj = mapObj

        for x in range(self.mapObj.width):
            for y in range(self.mapObj.height):
                img = self.mapObj.get_tile_image(x, y, self.index)
                if img:
                    newX = x * self.mapObj.tilewidth
                    newY = y * self.mapObj.tileheight
                    self.tiles.add(Tile(img, newX, newY))

    def draw(self, screen):
        self.tiles.draw(screen)


class Tile(pygame.sprite.Sprite):
    def __init__(self, image, x, y):
        pygame.sprite.Sprite.__init__(self)
        self.image = image
        self.rect = self.image.get_rect()
        self.rect.x = x
        self.rect.y = y


class SpriteSheet(object):
    def __init__(self, filename):
        self.sheet = pygame.image.load(filename).convert()

    def image_at(self, rect, colorkey=None):
        newRect = pygame.Rect(rect)
        image = pygame.Surface(newRect.size).convert()
        image.blit(self.sheet, (0, 0), newRect)
        if colorkey is not None:
            if colorkey is -1:
                colorkey = image.get_at((0, 0))
            image.set_colorkey(colorkey, pygame.RLEACCEL)
        return image


class Level():
    def __init__(self, filename):
        self.mapObj = load_pygame(filename)
        self.layers = []
        self.levelShift = 0

        for layer in range(len(self.mapObj.layers)):
            self.layers.append(TileLayer(layer, self.mapObj))

    def shiftLevelX(self, shiftX):
        self.levelShift += shiftX

        for layer in self.layers:
            for tile in layer.tiles:
                tile.rect.x += shiftX

    def shiftLevelY(self, shiftY):
        self.levelShift += shiftY

        for layer in self.layers:
            for tile in layer.tiles:
                tile.rect.y += shiftY

    def draw(self, screen):
        for layer in self.layers:
            layer.draw(screen)