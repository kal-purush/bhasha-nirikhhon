'''
Created on 17 Aug 2015

@author: David Monk
'''

import pygame, sys, os, copy
from pygame.locals import *

import Engine.conversion_tools as conv
import Engine.particle_classes as cls
import Engine.physics as phys
import numpy as np


pygame.init()

x = 0
y = 0
os.environ['SDL_VIDEO_WINDOW_POS'] = "%d,%d" %(x,y)

FPS = 60
fpsClock = pygame.time.Clock()
window = (1000,800)
centre_point = np.array([[int(window[0]/2)],[int(window[1]/2)]])

DISPLAYSURF = pygame.display.set_mode(window,0,32)
pygame.display.set_caption('Dodj_test')

BLACK       = (  0,   0,   0)
WHITE       = (255, 255, 255)
RED         = (255,   0,   0)
GREEN       = (  0, 255,   0)
BLUE        = (  0,   0, 255)
TRANSPARENT = (  0,   0,   0, 0)


def InitParticleGen(n,speed,window):
    particles = []
    for i in range(n):
        particles.append(cls.Particle(speed,window,i))
    return particles,n

def main(window):
    #CREATE GAME WINDOW
    GAME_WINDOW_rect = pygame.Rect((0, 0)+ window)
    GAME_WINDOW = pygame.Surface((GAME_WINDOW_rect.width, GAME_WINDOW_rect.height), 0, 32)
    GAME_WINDOW = GAME_WINDOW.convert_alpha()
    #CREATE MOUSE
    pygame.mouse.set_pos(centre_point)
    pygame.mouse.set_visible(False)
    mouse = cls.Mouse(centre_point)
    DISPLAYSURF.fill(WHITE)
    pygame.display.update()
    t, gameover = 0, False
    #CREATE INITIAL PARTICLES
    init_n = 3
    init_speed = 2
    particles, ID = InitParticleGen(init_n,init_speed,window)
    #GAME LOOP
    while True:
        GAME_WINDOW.fill(WHITE)
        dirty_rects = [copy.deepcopy(mouse.rect)]
        particles_to_delete = []
        #CREATE NEW PARTICLES
        n = int(init_n + 0.005*t)
        speed = 2 + 0.005*t
        while len(particles) < n:
            particles.append(cls.Particle(speed,window,ID))
            ID += 1
        #GET NEW MOUSE POSITION
        for event in pygame.event.get():
            if event.type == QUIT:            #Included here for efficiency
                pygame.quit()
                sys.exit()
            elif event.type == MOUSEMOTION:
                mouse.location = np.array(event.pos).reshape(2,1)
                mouse.rect.center = event.pos
        #DRAW MOUSEFalse,
        GAME_WINDOW.blit(mouse.image,mouse.rect)
        dirty_rects.append(mouse.rect)
        #DRAW PARTICLES
        for i in particles:
            dirty_rects.append(copy.deepcopy(i.rect))
            i.speed = speed
            i.maneuverability = 0.01 + speed/1000
            i.velocity,dist = phys.VelocityCalc(i,mouse)
            i.location += i.velocity
            i.rect.center = conv.ArraytoTuple(i.location)
            GAME_WINDOW.blit(i.image,i.rect)
            dirty_rects.append(i.rect)
            #TEST FOR GAMEOVER CONDITION
            if dist < mouse.radius + i.radius:
                gameover = True
            #TEST IF PARTICLE IS OUT OF WINDOW
            if i.location[0] < 0 - (i.radius + 1):
                particles_to_delete.append(i.ID)
            elif i.location[0] > window[0] + (i.radius + 1):
                particles_to_delete.append(i.ID)
            elif i.location[1] < 0 - (i.radius + 1):
                particles_to_delete.append(i.ID)
            elif i.location[1] > window[1] + (i.radius + 1):
                particles_to_delete.append(i.ID)
        #REMOVE PARTICLES OFF SCREEN
        for i in particles_to_delete:
            for j in range(len(particles)):
                if particles[j].ID == i:
                    del particles[j]
                    break
        #DRAW TIMER
        score = int(t/FPS)
        font_obj_time = pygame.font.Font('freesansbold.ttf',20)
        text_surface_obj_time = font_obj_time.render('%s'%score,
                                                                    True, BLACK)
        text_rect_obj_time = text_surface_obj_time.get_rect()
        TIMER = pygame.Surface((text_rect_obj_time.width, text_rect_obj_time.height),0 , 32)
        TIMER = TIMER.convert_alpha()
        TIMER.fill(TRANSPARENT)
        TIMER.blit(text_surface_obj_time, text_rect_obj_time)
        text_rect_obj_time.center = (window[0] - 30, 30)
        GAME_WINDOW.blit(TIMER, text_rect_obj_time)
        dirty_rects.append(pygame.Rect(window[0] - 50, 0, 50, 50))
        #UPDATE DISPLAY
        DISPLAYSURF.blit(GAME_WINDOW, GAME_WINDOW_rect)
        pygame.display.update(dirty_rects)
        t += 1
        fpsClock.tick(FPS)
        #GAMEOVER
        if gameover == True:
            print('Game over.\nYou lasted %s seconds.'%score)
            pygame.quit()
            sys.exit()


if __name__ == '__main__':
    main(window)