from pico2d import *
import math

open_canvas()

# fill here
grass = load_image('grass.png')
character = load_image('character.png')

def Character_Draw(Cx, Cy):
    clear_canvas_now()
    grass.draw_now(400, 30)
    character.draw_now(Cx, Cy)

while (1):
    x = 400
    y = 90
    circle_x = []
    circle_y = []
    
    while x < 800 - 20:
        Character_Draw(x, y)
        x += 5
        delay(0.001)

    while y < 600 - 40:
        Character_Draw(x, y)
        y += 5
        delay(0.001)

    while x > 0 + 20:
        Character_Draw(x, y)
        x -= 5
        delay(0.001)

    while y > 90:
        Character_Draw(x, y)
        y -= 5
        delay(0.001)

    while x < 400:
        Character_Draw(x, y)
        x += 5
        delay(0.001)
        
    for theta in range(-90, 270):
        x = (400 + 210*math.cos(math.radians(theta)))
        y = (300 + 210*math.sin(math.radians(theta)))
        Character_Draw(x, y)
        delay(0.001)

close_canvas()