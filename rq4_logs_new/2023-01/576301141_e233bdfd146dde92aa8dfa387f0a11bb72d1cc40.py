import pygame
from tiles import StaticTile, AnimatedTile
from settings import TILE_SIZE


class Diamond(AnimatedTile):
    path = "../graphics/stuff/animate_diamonds/"

    def __init__(self, position: tuple):
        super().__init__(position, Diamond.path)

        # поменяем позицию
        self.rect.x += TILE_SIZE // 2 - self.image.get_width() // 2
        self.rect.y += TILE_SIZE // 2 - self.image.get_height() // 2


class Box(StaticTile):
    path = "../graphics/decorations/box.png"

    def __init__(self, position: tuple):
        super().__init__(position, pygame.image.load(self.path).convert_alpha())

        # поменяем позицию
        self.rect.y += TILE_SIZE - self.image.get_height()
        self.rect.x += TILE_SIZE - self.image.get_width()