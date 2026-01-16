#PYGLET MOCKUP [WorkingPrototype ready]

# Pyglet, purpose of statement:
#   To provide a support for the graphical system
#   and most likey animation and sound systems later.
#   For a WP(WorkingPrototype) we will only be using
#   images and basic square shapes to populate the
#   graphical system.
#   This will, at the end of the dev-cycle, be migrated
#   to a soley image based system while any general shapes
#   will be given to the Collision system
#   Note: Pyglet WILL NOT support the collision system

import pyglet

window = pyglet.window.Window()

#resource path re-direct
pyglet.resource.path = ['../res/images']
pyglet.resource.reindex()


### DISPLAY DEMO

image = pyglet.resource.image('alf.jpg')
image.width = 100
image.height = 100
def display_img():
    image.blit(0,0)

label = pyglet.text.Label('Hello, world',font_name='Times New Roman',\
            font_size=36,x=window.width//2,y=window.height//2,\
            anchor_x='center',anchor_y='center')
def display_lbl():
    label.draw()

####

### GRAPHICS DEMO

import pyglet.graphics as pg
import pyglet.gl as gl

#Note: difference between .draw() and .draw_indexed()
#       .draw() draws singular shape, .draw_indexed()
#       can draw many while reusing points

def primitive_draw():
    pg.draw(2,gl.GL_POINTS,('v2i',(10,15,30,35)))

def square_draw():
    pg.draw_indexed(4,gl.GL_TRIANGLES,[0,1,2,0,2,3],('v2i',(100,100,150,100,150,150,100,150)))

###

#Note: When designing graphical system use squares and images,
#        the collision system will deal with cicles and squares

### KEYBOARD INPUT DEMO

#note: @window.event indicates built in func
from pyglet.window import key

@window.event
def on_key_press(symbol, modifiers):
    if symbol == key.A:
        print("A was pressed")
    elif symbol == key.LEFT:
        print("Left was pressed")
    elif symbol == key.ENTER:
        print("Enter was pressed")
    else:
        print('A key was pressed')

###


### MOUSE INPUT DEMO
from pyglet.window import mouse

@window.event
def on_mouse_press(x,y,button,modifiers):
    if button == mouse.LEFT:
        print("Click")
    if button == mouse.RIGHT:
        print("Clock")

###


###VISUAL LOOP

@window.event
def on_draw():
    window.clear()
    display_img()
    #display_lbl()
    #primitive_draw()
    #square_draw()

pyglet.app.run()

###