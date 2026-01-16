# Basic infrastructure for Bubble Shooter

import simplegui
import random
import math

# Global constants
WIDTH = 800
HEIGHT = 600
FIRING_POSITION = [WIDTH // 2, HEIGHT]
FIRING_LINE_LENGTH = 60
FIRING_ANGLE_VEL_INC = 0.02
BUBBLE_RADIUS = 20
COLOR_LIST = ["Red", "Green", "Blue", "White"]

# global variables
firing_angle = math.pi / 2
firing_angle_vel = 0
bubble_stuck = True
left = False
right = False
# helper functions to handle transformations
def angle_to_vector(ang):
    return [math.cos(ang), math.sin(ang)]

def dist(p,q):
    return math.sqrt((p[0]-q[0])**2+(p[1]-q[1])**2)


# class defintion for Bubbles
class Bubble:
    global firing_angle
    def __init__(self):
        self.pos = [WIDTH // 2, HEIGHT-BUBBLE_RADIUS-1]
        self.vel = [0.0, 0.0]
        self.color = random.choice(COLOR_LIST)
        self.sound = simplegui.load_sound('http://commondatastorage.googleapis.com/codeskulptor-assets/Epoq-Lepidoptera.ogg')
    
    def update(self):
        self.pos[0] += self.vel[0]
        self.pos[1] += self.vel[1]
        if self.pos[0] <= BUBBLE_RADIUS or self.pos[0] >= WIDTH-BUBBLE_RADIUS:
            self.vel[0] = -self.vel[0]
        if self.pos[1] <= BUBBLE_RADIUS or self.pos[1] >= HEIGHT-BUBBLE_RADIUS:
            self.vel[1] = -self.vel[1]
                
        
    def fire_bubble(self, vel):
        self.vel = vel
        self.sound.play()
    def is_stuck(self): 
        pass

    def collide(self, bubble):
        pass
            
    def draw(self, canvas):
        canvas.draw_circle(self.pos, BUBBLE_RADIUS, 2, self.color)
        

# define keyhandlers to control firing_angle
def keydown(key):
    global a_bubble, firing_angle, firing_angle_vel, bubble_stuck, left, right
    if key == simplegui.KEY_MAP['left']:
        left = True
    if key == simplegui.KEY_MAP['right']:
        right = True
    if key == simplegui.KEY_MAP['space']:
        a_bubble.fire_bubble(angle_to_vector(firing_angle)*2)

def keyup(key):
    global firing_angle_vel, left, right, a_bubble
    if key == simplegui.KEY_MAP['left']:
        left = False
        firing_angle_vel = 0
    if key == simplegui.KEY_MAP['right']:
        right = False
        firing_angle_vel = 0
    
# define draw handler
def draw(canvas):
    global left, firing_angle, a_bubble, bubble_stuck, firing_angle_vel, FIRING_ANGLE_VEL_INC
    
    # update firing angle
    if left:
        firing_angle_vel += FIRING_ANGLE_VEL_INC
        firing_angle += firing_angle_vel
    if right:
        firing_angle_vel += FIRING_ANGLE_VEL_INC
        firing_angle -= firing_angle_vel
    # draw firing line
    endpoint = [FIRING_POSITION[0]+FIRING_LINE_LENGTH*angle_to_vector(firing_angle)[0],FIRING_POSITION[1]-FIRING_LINE_LENGTH*angle_to_vector(firing_angle)[1]]
    canvas.draw_line(FIRING_POSITION, endpoint, 5, 'Blue')
    # update a_bubble and check for sticking  
    a_bubble.update()
    a_bubble.draw(canvas)
    # draw a bubble and stuck bubbles
 
# create frame and register handlers
frame = simplegui.create_frame("Bubble Shooter", WIDTH, HEIGHT)
frame.set_keydown_handler(keydown)
frame.set_keyup_handler(keyup)
frame.set_draw_handler(draw)
a_bubble = Bubble()
# create initial buble and start frame
frame.start()