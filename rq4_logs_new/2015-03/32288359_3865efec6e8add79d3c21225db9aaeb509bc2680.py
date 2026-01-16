'''Maybe someday this will generate and draw a Koch curve, but that turns
	out to be really hard. For now, it draws a triangle with a circle
	around it. **shrug** '''
import math
import pygame
import sys
from pygame.locals import *

WIDTH, HEIGHT = 800, 800
FPS = 30
ANGLE = 90
RADIUS = 30


# start_point = ((WIDTH / 2, HEIGHT / 2), 90, (0,0), (0,0), (0,0))
start_point = ((WIDTH / 2), (HEIGHT / 2))


pygame.init()
drawing_window = pygame.display.set_mode((WIDTH, HEIGHT))

fpsTime = pygame.time.Clock()

while True:
	for event in pygame.event.get():
		if event.type == pygame.QUIT:
			pygame.quit()
			sys.exit()
	#Locate vertices relative to start_point using trig functions
	v1x = start_point[0] + (math.cos(math.radians(ANGLE)) * RADIUS)
	v1y = start_point[1] + (math.sin(math.radians(ANGLE)) * RADIUS)
	v2x = start_point[0] + (math.cos(math.radians(ANGLE + 120)) * RADIUS)
	v2y = start_point[1] + (math.sin(math.radians(ANGLE + 120)) * RADIUS)
	v3x = start_point[0] + (math.cos(math.radians(ANGLE + 240)) * RADIUS)
	v3y = start_point[1] + (math.sin(math.radians(ANGLE + 240)) * RADIUS)
	#Locate point on circle with radius = RADIUS at degrees 0 through 360. Draw
	#single pixle-width rect at each point.
	for degree in range(1, 361):
		x2 = start_point[0] + (math.cos(math.radians(degree)) * RADIUS)
		y2 = start_point[1] + (math.sin(math.radians(degree)) * RADIUS)
		pygame.draw.rect(drawing_window, (255, 255, 255), (x2,y2, 1, 1), 1)
	pygame.draw.line(drawing_window, (255, 0, 0), (v1x, v1y), (v2x, v2y)) 
	pygame.draw.line(drawing_window, (0, 255, 0), (v2x, v2y), (v3x, v3y))
	pygame.draw.line(drawing_window, (0, 0, 255), (v3x, v3y), (v1x, v1y))

	# print(str(v1x) + ',' + str(v1y))
	pygame.display.update()	
	fpsTime.tick(FPS)

