import pygame
from helpers import create_coordinate, create_fruit_random_coordinates, match_check

import random


class Fruit:
    def __init__(self, width=800, height=600, step=5, radius=2, color=(255, 0, 0)):
        self.width = width
        self.eaten = False
        self.height = height
        self.step = step
        self.color = color
        self.radius = radius
        self.coordinates = None
        self.need_restart = False

        self.generate_fruit()

    def generate_fruit(self, snake_segments=[{"coordinates": {"x": 0, "y": 0}}]):
        self.need_restart = False
        new_coord = create_fruit_random_coordinates(
            self.width, self.height, self.step, snake_segments)

        fruit = create_coordinate(
            new_coord["x"], new_coord["y"], self.radius, (255, 0, 0))
        self.color = fruit["color"]
        self.coordinates = fruit["coordinates"]
        self.radius = fruit["radius"]

    def print_fruit(self, screen):
        pygame.draw.circle(
            screen, self.color, (self.coordinates["x"], self.coordinates["y"]), self.radius)

    def set_eaten(self):
        self.eaten = True

    def get_coordinates(self):
        return self.coordinates

    def set_need_restart(self, value):
        self.need_restart = value

    def get_need_restart(self):
        return self.need_restart