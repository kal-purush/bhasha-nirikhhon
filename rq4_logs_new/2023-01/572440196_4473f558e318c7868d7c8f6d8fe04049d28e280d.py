import pyglet
from random import randint

class Roller(pyglet.event.EventDispatcher):
    def __init__(self):
        super().__init__()
        self.register_event_type("rolled")

        self.tbatch = pyglet.graphics.Batch()
        self.bbatch = pyglet.graphics.Batch()

        self.pressed_button = False

        self.button = pyglet.shapes.Rectangle(300, 200, 100, 50, (255,0,0), batch=self.bbatch)
        self.label = pyglet.text.Label("Rul", anchor_x="center",font_size=30, x=350, y= 210, batch=self.tbatch)

        self.dice = [[[0,0]], ]

    def draw(self):
        self.bbatch.draw()
        self.tbatch.draw()

    def on_mouse_press(self, x, y, button, modifiers):
        if button == 1 and 300 <= x <= 400 and 200 <= y <= 250:
            self.pressed_button = True
            self.button.color = (200,0,0)
        else: self.pressed_button = False
        
    def on_mouse_release(self, x, y, button, modifiers):
        self.button.color = (255,0,0)
        if button == 1 and 300 <= x <= 400 and 200 <= y <= 250 and self.pressed_button:
            self.dispatch_event("rolled", randint(1,6) + randint(1,6))
            
