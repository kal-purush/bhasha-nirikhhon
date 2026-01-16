import math
import pygame, sys, colorsys
from pygame.locals import *

pygame.init()
WIDTH, HEIGHT = 700, 700 #alter the screen dimensions here
screen = pygame.display.set_mode((WIDTH, HEIGHT))

position = [0, 0]

def degrees_to_radians(angle):
    return angle * math.pi / 180 % (2 * math.pi)

def angle_to_point(start, length, angle):
    angle = degrees_to_radians(angle)
    start[0] += length * math.cos(angle)
    start[1] += length * math.sin(angle)
    start[0] = float("{0:.5f}".format(start[0]))
    start[1] = float("{0:.5f}".format(start[1]))
    return(start)

#initial angle = 0
a = 0
#initial huw = 0
h = 0

while True:
    #finds the coordiate along the circle to plot, try changing the setting the
    #centre of the circle to be a point along a different circle for interesting
    #effects eg:
    #x, y =  angle_to_point(angle_to_point([WIDTH/2,HEIGHT/2], 100, a*5), 50, a*0.25)
    #which create a donut
    #the following example just draws a cicle centered to the screen radius 100
    x, y =  angle_to_point([WIDTH/2,HEIGHT/2], 100, a)
    x = int(x) #converts the x and y coodiates into integers to be plotted
    y = int(y)
    h = h + 0.0005 % 1 #alters the hue but ensures it's never above 1, the smaller the increment the slower the gradient
    colour = colorsys.hsv_to_rgb(h,1,1) #converts the hue into an rgb value
    colour = (colour[0] * 255, colour[1] * 255, colour[2] * 255) #converts the rgb value into on pygame understands
    screen.set_at((x,y), colour) #you can either plot a single point using this line
    pygame.draw.circle(screen, colour, (x, y), 2) # or if you want a thicker line you can plot circles, alter the radius for different thicknesses
    a += 0.1 #increments the angle, the smaller this value, the more solid the line
    pygame.display.update() #updates the screen, if you want it to draw faster only update if i % a value == 0 eg:
    #if i % 18 ==0:
        #pygame.display.update()
    for event in pygame.event.get():
        if event.type == QUIT: #checks to see if you've pressed the x button
            pygame.quit() #if you have its shuts the program down
            sys.exit()