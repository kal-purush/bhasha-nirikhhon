"""
EID - H402752
Name - MAMIDALA SAI PRAHARSH
Institue - IIT Hyderabad

"""


import pygame
vec = pygame.math.Vector2

# BUTTON CLASS FOR TEMPLATE OF BUTTONS TO BE DISPLAYED ON THE SCREEN - RUN, RESET, PAUSE, RESUME 
class Button:
    # INIT FUNCTION FOR INITIALISATION WITH NECESSARY ATTRIBUTES NEEDED
    def __init__(self, surface, x, y, width, height, state='', function=0, colour=(255,255,255), hover_colour=(255,255,255), 
    border = True, border_width=2, border_colour=(0,0,0), text='', font_name='arial',text_size=20,  text_colour=(0,0,0), bold_text= False):
        self.type = 'button'
        self.surface = surface
        self.x = x
        self.y = y
        self.pos = vec(x,y)
        self.width = width
        self.height = height
        self.image = pygame.Surface((width, height))
        self.rect = self.image.get_rect()
        self.colour = colour
        #topleft is for positioning for pos
        self.rect.topleft = self.pos
        self.state = state
        self.function = function
        self.hover_colour = hover_colour
        self.border = border
        self.border_width = border_width
        self.border_colour = border_colour
        self.text = text
        self.font_name = font_name
        self.text_size = text_size
        self.text_colour = text_colour
        self.bold_text = bold_text
        self.hovered  = False
        self.showing = True

    # UPDATE FUNCTION TO MAKE THE BUTTON ACTIVE ONCE IT IS HOVERED BY MOUSE
    def update(self,pos, game_state=''):
        if self.mouse_hovered(pos):
            self.hovered = True
        else:
            self.hovered = False
        if self.state =='' or game_state=='':
            self.showing = True
        else:
            if self.state == game_state:
                self.showing = True
            else:
                self.showing = False

    # DRAW FUNCTION FOR BLITTING THE BUTTON IMAGE INTO THE SCREEN ONCE IT IS ACTIVE
    def draw(self):
        if self.showing:
            if self.border:
                self.image.fill(self.border_colour)
                if self.hovered:
                    pygame.draw.rect(self.image, self.hover_colour, (self.border_width, self.border_width, 
                                                                    self.width-(self.border_width*2), self.height-(self.border_width*2)))
                else:
                    pygame.draw.rect(self.image, self.colour, (self.border_width, self.border_width, 
                                                                    self.width-(self.border_width*2), self.height-(self.border_width*2)))
            else:
                self.image.fill(self.colour)
            if len(self.text) > 0:
                self.show_text()
            self.surface.blit(self.image, self.pos)

    # FUNCTION TO DIPLAY TEXT ON THE BUTTON
    def show_text(self):
        font = pygame.font.SysFont(self.font_name, self.text_size, bold=self.bold_text)
        text = font.render(self.text, False, self.text_colour)
        size = text.get_size()
        x = self.width//2-(size[0]//2)
        y = self.height//2-(size[1]//2)
        pos = vec(x,y) #not needed
        self.image.blit(text, pos)
    

    # FUNCTION FOR CALLING  THE FUNCITONS IN main.py WHEN BUTTON CLICKED ACCORDINGLY GIVEN AS 'function' ARGUMENT in INIT function.
    def click(self):
        if self.function != 0 and self.hovered:
            self.function()
    
    # FUNCTION FOR SETTING THE HOVER VARIABLE TO TRUE WHEN MOUSE HOVERED ON
    def mouse_hovered(self, pos):
        if self.showing:    
            if pos[0] > self.pos[0] and pos[0] < self.pos[0]+self.width:
                if pos[1] > self.pos[1] and pos[1] < self.pos[1]+self.height:
                    return True
            else:
                return False
        else:
            return False