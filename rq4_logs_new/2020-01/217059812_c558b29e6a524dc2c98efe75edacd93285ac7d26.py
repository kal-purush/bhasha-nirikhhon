import pygame
pygame.init()
screen = pygame.display.set_mode((400, 400))
done = False
warna_bg = (250,222,250)
pygame.display.set_caption("Garis UAS")
clock = pygame.time.Clock()
while not done:
		for event in pygame.event.get():
			if event.type == pygame.QUIT:
				done = True
			if event.type == pygame.KEYDOWN and \
					event.key == pygame.K_ESCAPE:
				done = True
		screen.fill(warna_bg)
		pygame.draw.line(screen, (250, 0, 0), (0, 0), (400, 400))
		pygame.draw.line(screen, (250, 0, 0),(400, 0,), (0, 400))
		pygame.draw.line(screen, (250, 0, 0),(200,400,), (200, 0))
		pygame.display.update()
		clock.tick(110)