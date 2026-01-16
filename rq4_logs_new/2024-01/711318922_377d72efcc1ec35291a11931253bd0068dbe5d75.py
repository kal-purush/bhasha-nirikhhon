import pygame
import abc
import utilities
import math


class Unit(abc.ABC):
    def __init__(self, w, surface, x, y, ownership, attack, defence, attack_range, speed):
        self.w = w
        self.surface = surface
        self.tile_x = x - 1
        self.tile_y = y - 1
        location = utilities.Utilities().center_square_tile_circle(w, x, y)
        self.x = location[0]
        self.y = location[1]
        self.ownership = ownership
        self.attack = attack
        self.defence = defence
        self.attack_range = attack_range
        self.speed = speed
        self.prev_x = self.tile_x
        self.prev_y = self.tile_y

    def update(self):
        if self.ownership == 'Red_Player':
            self.w.get_world_map()[self.prev_y][self.prev_x].init_tile()
            pygame.draw.circle(self.surface, (255, 0, 0), (self.x, self.y), 5)
        elif self.ownership == 'Blue_Player':
            self.w.get_world_map()[self.prev_y][self.prev_x].init_tile()
            pygame.draw.circle(self.surface, (0, 0, 255), (self.x, self.y), 5)

    def move_x(self, tiles):
        self.prev_x = self.tile_x
        self.prev_y = self.tile_y
        self.tile_x += tiles
        self.x += self.w.get_tile_size() * tiles

    def move_y(self, tiles):
        self.prev_x = self.tile_x
        self.prev_y = self.tile_y
        self.tile_y += tiles
        self.y += self.w.get_tile_size() * tiles
