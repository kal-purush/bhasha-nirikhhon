import pygame 

pygame.init()

width , height = 500 , 500
screen = pygame.display.set_mode((width , height))
pygame.display.set_caption('ball game')

red = (255 , 0 , 0)
white = (255 , 255 ,255)
running = True

x_axis = 250 
y_axis = 250 
r = 25 
speed = 20



while running:
    screen.fill(white)
    pygame.draw.circle(screen , red  , (x_axis , y_axis) , r )

    pygame.display.flip()
    
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_UP:
                if y_axis - speed >= r:
                    y_axis -= speed
            elif event.key == pygame.K_DOWN:
                if y_axis + speed <= height - r:
                    y_axis += speed 
            elif event.key == pygame.K_LEFT:
                if x_axis - speed >= r:
                    x_axis -= speed
            elif event.key == pygame.K_RIGHT:
                if x_axis + speed <= width - r:
                    x_axis += speed
   



pygame.quit()