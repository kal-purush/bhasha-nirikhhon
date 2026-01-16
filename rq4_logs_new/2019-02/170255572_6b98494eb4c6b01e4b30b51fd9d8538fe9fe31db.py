import pygame

pygame.init()

white = (255,255,255)
black = (0,0,0)

red = (255, 0, 0)
green = (0, 255, 0)
blue = (0 ,0, 255)

gameDisplay = pygame.display.set_mode((800, 600))

gameDisplay.fill(black)

pixAr = pygame.PixelArray(gameDisplay)
pixAr[10][20] = green

pygame.draw.line(gameDisplay, green, (100, 200), (300, 450), 5)
#                where        colour  start       end        thickness
pygame.draw.rect(gameDisplay, red, (400, 400, 50, 25))
#                where        colour, x, y, width, height
pygame.draw.circle(gameDisplay, white, (150, 150), 75)

pygame.draw.polygon(gameDisplay, blue, ((25, 75), (76, 125), (250, 375), (400, 25), (60, 540)))

while True:
	
	for event in pygame.event.get():
		if event.type == pygame.QUIT:
			pygame.quit()
			quit()
			
		pygame.display.update()	
		
		#making pixel arrays