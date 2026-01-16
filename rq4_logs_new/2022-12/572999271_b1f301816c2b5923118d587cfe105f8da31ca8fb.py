import pygame
import math
import itertools


class Forces:

    def __init__(self,
                 collision=True,
                 surface=True,
                 gravity=True,
                 magnetic=True
                 ):
        self.collision = collision
        self.surface = surface
        self.gravity = gravity
        self.magnetic = magnetic

    def collision_forces(self, objects):
        if self.collision:
            for s1, s2 in itertools.combinations(objects, 2):
                if s1.shape == s2.shape == "Square":
                    rect1 = pygame.Rect((s1.x, s1.y), (s1.size, s1.size))
                    if rect1.colliderect(s2.x, s2.y, s2.size, s2.size):
                        s1.x_vel *= -1
                        s2.x_vel *= -1
                if s1.shape == s2.shape == "Circle":
                    distance = math.sqrt((s2.x - s1.x)**2 + (s2.y-s1.y)**2)
                    if distance < max([s1.radius, s2.radius]):
                        s1.x_vel *= -1
                        s2.x_vel *= -1

    def surface_forces(self, objects, surfaces):
        for surface in surfaces:
            for obj in objects:
                if obj.shape == "Rectangle":
                    if surface.colliderect(obj.x, obj.y+obj.y_vel, obj.size, obj.size):
                        obj.y_vel *= -1
                    if surface.colliderect(obj.x+obj.x_vel, obj.y, obj.size, obj.size):
                        obj.x_vel *= -1
                if obj.shape == "Circle":
                    pass

    def gravity_forces(self):
        pass

    def magnetic_forces(self):
        pass

    def summation_forces(self):
        pass