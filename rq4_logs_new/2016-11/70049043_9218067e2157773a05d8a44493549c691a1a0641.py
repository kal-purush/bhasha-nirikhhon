# program template for Spaceship
import simplegui
import math
import random

# globals for user interface
WIDTH = 1400
HEIGHT = 900
score = 0
lives = 3
time = 0

class ImageInfo:
    def __init__(self, center, size, radius = 0, lifespan = None, animated = False):
        self.center = center
        self.size = size
        self.radius = radius
        if lifespan:
            self.lifespan = lifespan
        else:
            self.lifespan = float('inf')
        self.animated = animated

    def get_center(self):
        return self.center

    def get_size(self):
        return self.size

    def get_radius(self):
        return self.radius

    def get_lifespan(self):
        return self.lifespan

    def get_animated(self):
        return self.animated

    
# art assets created by Kim Lathrop, may be freely re-used in non-commercial projects, please credit Kim
    
# debris images - debris1_brown.png, debris2_brown.png, debris3_brown.png, debris4_brown.png
#                 debris1_blue.png, debris2_blue.png, debris3_blue.png, debris4_blue.png, debris_blend.png
debris_info = ImageInfo([320, 240], [640, 480])
debris_image = simplegui.load_image("http://commondatastorage.googleapis.com/codeskulptor-assets/lathrop/debris2_blue.png")

# nebula images - nebula_brown.png, nebula_blue.png
nebula_info = ImageInfo([400, 300], [800, 600])
nebula_image = simplegui.load_image("http://commondatastorage.googleapis.com/codeskulptor-assets/lathrop/nebula_blue.f2014.png")

# splash image
splash_info = ImageInfo([200, 150], [400, 300])
splash_image = simplegui.load_image("http://commondatastorage.googleapis.com/codeskulptor-assets/lathrop/splash.png")

# ship image
my_ship_info = ImageInfo([45, 45], [90, 90], 35)
enemy_ship_info = ImageInfo([45, 45], [90, 90], 35)
enemy_ship_image = simplegui.load_image("https://doc-0c-2s-docs.googleusercontent.com/docs/securesc/ha0ro937gcuc7l7deffksulhg5h7mbp1/rone6rheiqi2sj2rebi1b2ap5s6mdr43/1479304800000/02434405332829745180/*/0B3pfB7dP7Gf3OENHd1VxZWVodlU")
#ship_image = simplegui.load_image("http://commondatastorage.googleapis.com/codeskulptor-assets/lathrop/double_ship.png")
my_ship_image = simplegui.load_image("https://doc-08-2s-docs.googleusercontent.com/docs/securesc/ha0ro937gcuc7l7deffksulhg5h7mbp1/u95cdpicdh3141691612anbn3drch2ij/1479304800000/02434405332829745180/*/0B3pfB7dP7Gf3cERDaFgta0hqMFU")
# missile image - shot1.png, shot2.png, shot3.png
missile_info = ImageInfo([5,5], [10, 10], 3, 50)
missile_image = simplegui.load_image("http://commondatastorage.googleapis.com/codeskulptor-assets/lathrop/shot2.png")

# asteroid images - asteroid_blue.png, asteroid_brown.png, asteroid_blend.png
asteroid_info = ImageInfo([45, 45], [90, 90], 40)
asteroid_image = simplegui.load_image("http://commondatastorage.googleapis.com/codeskulptor-assets/lathrop/asteroid_blue.png")

# animated explosion - explosion_orange.png, explosion_blue.png, explosion_blue2.png, explosion_alpha.png
explosion_info = ImageInfo([64, 64], [128, 128], 17, 24, True)
explosion_image = simplegui.load_image("http://commondatastorage.googleapis.com/codeskulptor-assets/explosion.hasgraphics.png")

# sound assets purchased from sounddogs.com, please do not redistribute
soundtrack = simplegui.load_sound("http://commondatastorage.googleapis.com/codeskulptor-assets/sounddogs/soundtrack.mp3")
my_missile_sound = simplegui.load_sound("http://commondatastorage.googleapis.com/codeskulptor-assets/sounddogs/missile.mp3")
my_missile_sound.set_volume(.5)
enemy_missile_sound = simplegui.load_sound("http://commondatastorage.googleapis.com/codeskulptor-assets/sounddogs/missile.mp3")
enemy_missile_sound.set_volume(.5)

my_ship_thrust_sound = simplegui.load_sound("http://commondatastorage.googleapis.com/codeskulptor-assets/sounddogs/thrust.mp3")
enemy_ship_thrust_sound = simplegui.load_sound("http://commondatastorage.googleapis.com/codeskulptor-assets/sounddogs/thrust.mp3")
explosion_sound = simplegui.load_sound("http://commondatastorage.googleapis.com/codeskulptor-assets/sounddogs/explosion.mp3")

angle_inc = 0.05
vel_inc = 0.1
friction = 0.01
shooted_missiles = set([])
explosion_time = 0
my_ship_explosion = False
enemy_ship_explosion = False
myscore = 0
enemyscore = 0
mylife = 5
enemylife = 5
remove_missiles = []
my_ship_collide = False
enemy_ship_collide = False
explosion_missile = []
is_start = False
start_image = simplegui.load_image('https://doc-10-2s-docs.googleusercontent.com/docs/securesc/ha0ro937gcuc7l7deffksulhg5h7mbp1/pd2rbokc4rm5s6kho46rrlcpk10pn9f3/1479297600000/02434405332829745180/*/0B3pfB7dP7Gf3VEtjN3hUaUc4RDQ')
# alternative upbeat soundtrack by composer and former IIPP student Emiel Stopler
# please do not redistribute without permission from Emiel at http://www.filmcomposer.nl
#soundtrack = simplegui.load_sound("https://storage.googleapis.com/codeskulptor-assets/ricerocks_theme.mp3")

# helper functions to handle transformations
def angle_to_vector(ang):
    return [math.cos(ang), math.sin(ang)]

def dist(p,q):
    return math.sqrt((p[0] - q[0]) ** 2+(p[1] - q[1]) ** 2)


# Ship class
class Ship:
    def __init__(self, pos, vel, angle, image, info, sound, is_my_ship):
        self.type = is_my_ship
        self.pos = [pos[0],pos[1]]
        self.vel = [vel[0],vel[1]]
        self.thrust = False
        self.angle = angle
        self.angle_vel = 0
        self.image = image
        self.image_center = info.get_center()
        self.image_size = info.get_size()
        self.info = info
        self.radius = info.get_radius()
        self.sound = sound
        
    def draw(self,canvas):
        canvas.draw_circle(self.pos, self.radius, 1, "White", "Transparent")
        canvas.draw_image(self.image, self.info.get_center(), self.info.get_size(), self.pos, (self.radius*2, self.radius*2),self.angle+math.pi/2)
    def update(self):
        self.angle += self.angle_vel 
        if self.thrust:
            self.sound.play()
            self.image_center = [135, 45]
            self.info.center = [135, 45]
            self.vel[0] += vel_inc * angle_to_vector(self.angle)[0]
            self.vel[1] += vel_inc * angle_to_vector(self.angle)[1]
        if not self.thrust:
            self.image_center = [45, 45]
            self.info.center = [45, 45]
            self.sound.rewind()
        self.vel[0] *= (1-friction)
        self.vel[1] *= (1-friction)
        self.pos[0] += self.vel[0]
        self.pos[1] += self.vel[1]
        #avoid the ship getting off the edge
        self.pos[0] = self.pos[0] % WIDTH
        self.pos[1] = self.pos[1] % HEIGHT
    def shoot(self):
       global a_missile
       missile_pos = [0,0]
       missile_pos[0] = self.pos[0]+angle_to_vector(self.angle)[0]*self.radius
       missile_pos[1] = self.pos[1]+angle_to_vector(self.angle)[1]*self.radius
       ship_velocity = math.sqrt(self.vel[0] **2 +self.vel[1] **2)+2
       missile_vel = [angle_to_vector(self.angle)[0] * ship_velocity, angle_to_vector(self.angle)[1] * ship_velocity]
       if self.type:
            missile_sound = my_missile_sound
       else:
            missile_sound = enemy_missile_sound
       a_missile = Sprite(missile_pos, missile_vel, 0, 0, missile_image, missile_info, self.type, missile_sound)
       shooted_missiles.add(a_missile)
        
    def collide(self, missile):
        collide = False
        distance = dist(missile.pos, self.pos)
        if missile.type != self.type and  distance <= (self.radius+missile.radius):
            collide = True
        return collide
        
# Sprite class
class Sprite:
    def __init__(self, pos, vel, ang, ang_vel, image, info, is_my_ship, sound):
        self.pos = [pos[0],pos[1]]
        self.vel = [vel[0],vel[1]]
        self.angle = ang
        self.angle_vel = ang_vel
        self.image = image
        self.image_center = info.get_center()
        self.image_size = info.get_size()
        self.radius = info.get_radius()
        self.lifespan = info.get_lifespan()
        self.animated = info.get_animated()
        self.age = 0
        self.type = is_my_ship
        if sound:
            self.sound = sound
            self.sound.rewind()
            self.sound.play()
   
    def draw(self, canvas):
        if self.type:
            color = "Red"
        else:
            color = "Blue"
        canvas.draw_circle(self.pos, self.radius, 1, "White", color)
        canvas.draw_image(self.image, asteroid_info.get_center(), asteroid_info.get_size(), self.pos, (self.radius*2, self.radius*2), self.angle)
        
    def update(self):
        self.angle += self.angle_vel 
        self.pos[0] += self.vel[0]
        self.pos[1] += self.vel[1]
    

    
def draw_explosion(canvas, missile, is_my):
    global explosion_time, my_ship_collide, enemy_ship_collide, explosion_missile
    EXPLOSION_SIZE = [100, 100]
    EXPLOSION_DIM = [9, 9]
    EXPLOSION_CENTER = [50, 50]
    position = missile.pos
    image_size = [50, 50]
    if explosion_time <= 70:
        explosion_index = [explosion_time % EXPLOSION_DIM[0], (explosion_time // EXPLOSION_DIM[0]) % EXPLOSION_DIM[1]]
        canvas.draw_image(explosion_image, 
             [EXPLOSION_CENTER[0] + explosion_index[0] * EXPLOSION_SIZE[0], 
              EXPLOSION_CENTER[1] + explosion_index[1] * EXPLOSION_SIZE[1]], 
              EXPLOSION_SIZE, position, image_size)
        explosion_time += 1
    else: 
        explosion_time = 0
        if is_my:
            my_ship_collide = False
        else:
            enemy_ship_collide = False
        explosion_missile = []

def initial_game():
    # initialize ship and two sprites
    global my_ship, enemy_ship, a_missile, mylife, enemylife, shooted_missiles, remove_missiles
    my_ship = Ship([WIDTH*2 / 3, HEIGHT / 2], [0, 0], -math.pi/2, my_ship_image, my_ship_info, my_ship_thrust_sound, True)
    enemy_ship = Ship([WIDTH/ 3, HEIGHT / 2], [0, 0], -math.pi/2, enemy_ship_image, enemy_ship_info, enemy_ship_thrust_sound, False)
    a_missile = Sprite([WIDTH+5, HEIGHT+5], [0,0], 0, 0, missile_image, missile_info,True, my_missile_sound)
    mylife = 5
    shooted_missiles.add(a_missile)
    remove_missiles.append(a_missile)
    enemylife = 5
        
        
def draw(canvas):
    global start_image, explosion_missile, time, shooted_missiles, explosion_time, mylife, enemylife, myscore, enemyscore, remove_missiles, my_ship_collide, enemy_ship_collide
    # animiate background
    time += 1
    wtime = (time / 4) % WIDTH
    center = debris_info.get_center()
    size = debris_info.get_size()
    canvas.draw_image(nebula_image, nebula_info.get_center(), nebula_info.get_size(), [WIDTH / 2, HEIGHT / 2], [WIDTH, HEIGHT])
    canvas.draw_image(debris_image, center, size, (wtime - WIDTH / 2, HEIGHT / 2), (WIDTH, HEIGHT))
    canvas.draw_image(debris_image, center, size, (wtime + WIDTH / 2, HEIGHT / 2), (WIDTH, HEIGHT))
    
    if is_start:
        # draw ship and sprites
        my_ship.draw(canvas)
        enemy_ship.draw(canvas)
        if shooted_missiles != None:
            for missile in shooted_missiles:
                missile.draw(canvas)

        # update ship and sprites
        my_ship.update()
        enemy_ship.update()
        if shooted_missiles != None:
            for missile in shooted_missiles:
                missile.update()
                # remove missiles which are already out of edges
                if missile.pos[0] < 0 or missile.pos[0] > WIDTH:
                    remove_missiles.append(missile)
                if missile.pos[1] <0 or missile.pos[1] > HEIGHT:
                    remove_missiles.append(missile)    

        #draw explosion
        #draw_explosion(canvas, [50, 50], time)
        #draw_explosion(canvas, [150, 150], time)

        for missile in shooted_missiles:
            if my_ship.collide(missile):
                my_ship_collide = True
                print "my_ship collide!!!"
                explosion_sound.rewind()
                explosion_sound.play()
                explosion_missile.append(missile)
                remove_missiles.append(missile)
                mylife -= 1

            if enemy_ship.collide(missile):
                enemy_ship_collide = True
                explosion_sound.rewind()
                explosion_sound.play()
                explosion_missile.append(missile)
                remove_missiles.append(missile)
                enemylife -= 1

        if my_ship_collide:
                for each in explosion_missile:
                    draw_explosion(canvas, each, True)
        if enemy_ship_collide:
                for each in explosion_missile:
                    draw_explosion(canvas, each, False)

        #remove the missiles which should be disappeared
        for missile in remove_missiles:
            if missile in shooted_missiles:
                shooted_missiles.remove(missile)


        if mylife <= 0: 
            enemyscore +=1
            initial_game()
        if enemylife <= 0:
            myscore +=1
            initial_game()

        #draw score
        canvas.draw_text("Score: " + str(enemyscore), (60, 100), 20, 'White')
        canvas.draw_text("Score: " + str(myscore), (1200, 100), 20, 'White')
        canvas.draw_text("Mengmeng VS Liam", (450, 100), 60, 'Red')
        
    else:
        canvas.draw_polygon([[300, 200], [1100, 200], [1100, 700], [300, 700]], 8, 'Blue', 'Transparent')
        canvas.draw_text("Click Here to Start", (550, 600), 40, 'White')
        canvas.draw_image(start_image, (2154 / 2, 1125 / 2), (2154, 1125), (700, 400), (400, 200))
        canvas.draw_text("Mengmeng VS Liam", (450, 100), 60, 'Red')
        
# keydown keyup handler
def key_down(key):
    global my_ship, angle_inc, a_missile, shooted_missile, enemy_ship
    if key == simplegui.KEY_MAP['left']:
       my_ship.angle_vel -= angle_inc
    if key == simplegui.KEY_MAP['right']:
       my_ship.angle_vel += angle_inc
    if key == simplegui.KEY_MAP['up']:
       my_ship.thrust = True
    if key == simplegui.KEY_MAP['down']:
       my_ship.shoot()
        
    if key == simplegui.KEY_MAP['a']:
       enemy_ship.angle_vel -= angle_inc
    if key == simplegui.KEY_MAP['d']:
       enemy_ship.angle_vel += angle_inc
    if key == simplegui.KEY_MAP['w']:
       enemy_ship.thrust = True
    if key == simplegui.KEY_MAP['s']:
       enemy_ship.shoot()

def key_up(key):
    global my_ship, angle_inc, enemy_ship,  my_ship_thrust_sound, enemy_ship_thrust_sound
    if key == simplegui.KEY_MAP['left']:
        my_ship.angle_vel = 0
    if key == simplegui.KEY_MAP['right']:
        my_ship.angle_vel = 0
    if key == simplegui.KEY_MAP['up']:
       my_ship.thrust = False
    
    if key == simplegui.KEY_MAP['a']:
        enemy_ship.angle_vel = 0
    if key == simplegui.KEY_MAP['d']:
        enemy_ship.angle_vel = 0
    if key == simplegui.KEY_MAP['w']:
       enemy_ship.thrust = False

#mouse handler to handle player's click in the beginning
def mouse_handler(position):
    global is_start
    if position[0] in range (300, 1100):
        if position[1] in range (200, 700):
            is_start = True

# initialize frame
frame = simplegui.create_frame("Asteroids", WIDTH, HEIGHT)
initial_game()

# register handlers
frame.set_draw_handler(draw)
frame.set_keydown_handler(key_down)
frame.set_keyup_handler(key_up)
frame.set_mouseclick_handler(mouse_handler)

# get things rolling
frame.start()