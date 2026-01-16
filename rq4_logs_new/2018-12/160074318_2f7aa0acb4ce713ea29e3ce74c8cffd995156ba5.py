"""
Sprite Collect Coins


Artwork from http://kenney.nl

http://arcade.academy/examples/index.html

"""
import arcade
import random
import pymunk
import os

SCREEN_WIDTH = 800
SCREEN_HEIGHT = 600

COIN_SCALE = 0.2
COIN_COUNT = 30


VIEWPORT_MARGIN = 40
RIGHT_MARGIN = 150

MOVEMENT_SPEED = 2

MAP=[[1,1,1,1,1,1,1,1,1,1,1,1,1],
     [0,0,0,0,0,0,0,0,1,1,0,0,0],
     [0,0,0,0,0,0,0,0,0,0,0,0,0],
     [1,1,1,1,1,1,1,1,1,1,1,1,1],
     ]

def detect_collision(player,items_list):
    #brute force method
    ret_index=[]
    p_width,p_height=(player.width,player.height)
    p_x,p_y=player.get_position()
    i=0
    for item in items_list:
        i_width,i_height=(item.width,item.height)
        i_x,i_y=item.get_position()
        if (p_x < (i_x + i_width) and (p_x + p_width) > i_x and \
           p_y < (i_y + i_height) and p_height + p_y > i_y):
            ret_index.append(item)
        i+=1
    return ret_index





class Coin(arcade.AnimatedTimeSprite):
    """ Player class """

    def __init__(self):#, image, scale):
        """ Set up the player """

        # Call the parent init
        super().__init__()#image, scale)
        self.textures = []
        self.center_x = random.randrange(SCREEN_WIDTH)
        self.center_y = random.randrange(SCREEN_HEIGHT)
        #self.position=(self.center_x,self.center_y)
        coin_img_path="img/silver_coins/Silver_%i.png"
        for y in range(1,11):
            self.textures.append(arcade.load_texture(coin_img_path % y, scale=0.1))
        #for y in range(10,0,-1):
        #    self.textures.append(arcade.load_texture(attack_img % y,scale=0.1))
        self.cur_texture_index = random.randrange(len(self.textures))

        # Create a variable to hold our speed. 'angle' is created by the parent
        self.speed = 0


class Zombie(arcade.AnimatedTimeSprite):
    """ Player class """

    def __init__(self,gender="male"):#, image, scale):
        """ Set up the player """

        # Call the parent init
        super().__init__()#image, scale)
        self.walk_direction=1
        self.walk_from_x=0
        self.walk_to_x=0
        self.textures_walk_left=[]
        self.textures_walk_right=[]
        self.textures_idle_left=[]
        self.textures_idle_right=[]
        self.textures_attack_right=[]
        self.textures_attack_left=[]
        self.textures_dead_right=[]
        self.textures_dead_left=[]
        self.textures = []
        self.center_x = random.randrange(SCREEN_WIDTH)
        self.center_y = random.randrange(SCREEN_HEIGHT)
        attack_img="img/" +gender +"/Attack (%i).png"
        walk_img="img/" +gender +"/Walk (%i).png"
        idle_img="img/" +gender +"/Idle (%i).png"
        dead_img="img/" +gender +"/Dead (%i).png"
        self.direction="left"
        for y in range(1,9):
            self.textures_attack_right.append(arcade.load_texture(attack_img % y, scale=COIN_SCALE))
            self.textures_attack_left.append(arcade.load_texture(attack_img % y,mirrored=True, scale=COIN_SCALE))
        for y in range(1,11):
            self.textures_walk_right.append(arcade.load_texture(walk_img % y, scale=COIN_SCALE))
            self.textures_walk_left.append(arcade.load_texture(walk_img % y,mirrored=True, scale=COIN_SCALE))
        for y in range(1,16):
            self.textures_idle_left.append(arcade.load_texture(idle_img % y,mirrored=True, scale=COIN_SCALE))
            self.textures_idle_right.append(arcade.load_texture(idle_img % y, scale=COIN_SCALE))
        for y in range(1,13):
            self.textures_dead_left.append(arcade.load_texture(dead_img % y,mirrored=True, scale=COIN_SCALE))
            self.textures_dead_right.append(arcade.load_texture(dead_img % y, scale=COIN_SCALE))

        self.textures=self.textures_walk_left
        self.cur_texture_index = random.randrange(len(self.textures))

        # Create a variable to hold our speed. 'angle' is created by the parent
        self.speed = 0

    def get_last_side(self):
        return self.action.split("_")[0]

    def get_action(self):
        return self.action

    def set_action(self, action):
        self.action=action
        if action == "left_walk":
            self.textures=self.textures_walk_left
        elif action == "right_walk":
            self.textures=self.textures_walk_right
        elif action == "left_idle":
            self.textures = self.textures_idle_left
        elif action == "right_idle":
            self.textures = self.textures_idle_right
        elif action == "left_attack":
            self.textures = self.textures_attack_left
        elif action == "right_attack":
            self.textures = self.textures_attack_right

    def update(self):
        #self.center_x += MOVEMENT_SPEED
        print("update",self.center_x,self.walk_direction,self.walk_direction*MOVEMENT_SPEED)
        if self.center_x<=self.walk_from_x:
            self.walk_direction=+1.0
            self.set_action("right_walk")
            print("right_walk")
        if self.center_x>=self.walk_to_x:
            print("left_walk")
            self.walk_direction=-1.0
            self.set_action("left_walk")
        #print()
        self.center_x+=self.walk_direction*MOVEMENT_SPEED


class Player(arcade.AnimatedTimeSprite):
    """ Player class """

    def __init__(self,scale=COIN_SCALE):#, image, scale):
        """ Set up the player """

        # Call the parent init
        super().__init__()#image, scale)
        self.textures_walk_left=[]
        self.textures_walk_right=[]
        self.textures_idle_left=[]
        self.textures_idle_right=[]
        self.textures_attack_right=[]
        self.textures_attack_left=[]
        self.textures_dead_right=[]
        self.textures_dead_left=[]
        self.textures_jump_right=[]
        self.textures_jump_left=[]
        self.textures = []
        self.center_x = random.randrange(SCREEN_WIDTH)
        self.center_y = random.randrange(SCREEN_HEIGHT)
        attack_img="img/sheriff_girl/Shoot (%i).png"
        walk_img="img/sheriff_girl/Run (%i).png"
        idle_img="img/sheriff_girl/Idle (%i).png"
        dead_img="img/sheriff_girl/Dead (%i).png"
        jump_img="img/sheriff_girl/Jump (%i).png"
        self.action="left_idle"

        for y in range(1,4):
            self.textures_attack_right.append(arcade.load_texture(attack_img % y, scale=COIN_SCALE))
            self.textures_attack_left.append(arcade.load_texture(attack_img % y,mirrored=True, scale=COIN_SCALE))
        for y in range(1,9):
            self.textures_walk_right.append(arcade.load_texture(walk_img % y, scale=COIN_SCALE))
            self.textures_walk_left.append(arcade.load_texture(walk_img % y,mirrored=True, scale=COIN_SCALE))
        for y in range(1,11):
            self.textures_idle_left.append(arcade.load_texture(idle_img % y,mirrored=True, scale=COIN_SCALE))
            self.textures_idle_right.append(arcade.load_texture(idle_img % y, scale=COIN_SCALE))
        for y in range(1,11):
            self.textures_dead_left.append(arcade.load_texture(dead_img % y,mirrored=True, scale=COIN_SCALE))
            self.textures_dead_right.append(arcade.load_texture(dead_img % y, scale=COIN_SCALE))
        for y in range(1,11):
            self.textures_jump_left.append(arcade.load_texture(jump_img % y,mirrored=True, scale=COIN_SCALE))
            self.textures_jump_right.append(arcade.load_texture(jump_img % y, scale=COIN_SCALE))

        self.textures=self.textures_idle_left
        self.cur_texture_index = random.randrange(len(self.textures))

        # Create a variable to hold our speed. 'angle' is created by the parent
        self.speed = 0

    def get_last_side(self):
        return self.action.split("_")[0]

    def get_action(self):
        return self.action

    def set_action(self, action):
        self.action=action
        if action == "left_walk":
            self.textures=self.textures_walk_left
        elif action == "right_walk":
            self.textures=self.textures_walk_right
        elif action == "left_idle":
            self.textures = self.textures_idle_left
        elif action == "right_idle":
            self.textures = self.textures_idle_right
        elif action == "left_attack":
            self.textures = self.textures_attack_left
        elif action == "right_attack":
            self.textures = self.textures_attack_right
        elif action == "left_jump":
            self.textures = self.textures_jump_left
        elif action == "right_jump":
            self.textures = self.textures_jump_right
        elif action == "left_dead":
            self.textures = self.textures_dead_left
        elif action == "right_dead":
            self.textures = self.textures_dead_right



class MyGame(arcade.Window):
    """ Main application class. """

    def __init__(self, width, height):
        """
        Initializer
        """
        super().__init__(width, height)

        # Set the working directory (where we expect to find files) to the same
        # directory this .py file is in. You can leave this out of your own
        # code, but it is needed to easily run the examples using "python -m"
        # as mentioned at the top of this program.
        file_path = os.path.dirname(os.path.abspath(__file__))
        os.chdir(file_path)

        """ Set up the game and initialize the variables. """

        # Sprite lists
        self.all_sprites_list = None
        self.coin_list = None
        self.wall_list = None
        self.zombie_list = None
        self.physics_engine = None

        # Set up the player
        self.score = 0
        self.player = None
        self.key_pressed=False
        self.view_left = 0
        self.view_bottom = 0

    def setup(self):
        self.all_sprites_list = arcade.SpriteList()
        self.coin_list = arcade.SpriteList()
        self.zombie_list = arcade.SpriteList()

        #self.physics_engine = arcade.PhysicsEngineSimple(self.player)

        # Set up the player
        self.score = 0
        #self.player = arcade.AnimatedWalkingSprite()
        self.wall_list = arcade.SpriteList()
        character_scale = 0.3
        SPRITE_SCALING= 0.5
        # crete enemy
        zombie=Zombie("female")
        zombie.set_action("right_walk")
        zombie.walk_from_x=100
        zombie.walk_to_x=300
        zombie.set_position(200,120)
        self.zombie_list.append(zombie)
        self.all_sprites_list.append(zombie)

        zombie=Zombie("male")
        zombie.set_action("right_walk")
        zombie.walk_from_x=400
        zombie.walk_to_x=700
        zombie.set_position(200,120)
        self.zombie_list.append(zombie)
        self.all_sprites_list.append(zombie)

        # -- Set up the walls
        # Create a row of boxes

        #x_i=0
        x_index=0
        y_index=0
        for y_arr in MAP:
            #x_index=MAP.index(x_arr)
            for item in y_arr:
                #y_index=x_arr.index(item)
                if item==1:
                    pass
                    #wall = arcade.Sprite("boxCrate_double.png", SPRITE_SCALING)
                    #wall.top = (7-x_index)*64
                    #wall.right = y_index*64
                    #self.wall_list.append(wall)
                x_index+=1
                print(x_index,y_index)
            y_index+=1

        for x in range(0, 1500, 64):
            wall = arcade.Sprite("img/land/boxCrate_double.png", SPRITE_SCALING)
            print(wall.width)
            wall.center_x = x
            wall.center_y = 64/2
            self.wall_list.append(wall)

        # Create a column of boxes
        #for y in range(273, 500, 64):
        #    wall = arcade.Sprite("boxCrate_double.png", SPRITE_SCALING)
        #    wall.center_x = 465
        #    wall.center_y = y
        #    self.wall_list.append(wall)


        self.player=Player()
        self.player.texture_change_distance = 20


        self.player.center_x = SCREEN_WIDTH // 2
        self.player.center_y = SCREEN_HEIGHT // 2
        self.player.scale = 0.8

        self.all_sprites_list.append(self.player)
        print(type(self.player))
        #self.all_sprites_list.append(self.wall_list)

        counter=0
        for i in range(COIN_COUNT):
            while True:
                coin = Coin()
                collision=detect_collision(coin,self.coin_list)
                counter+=1
                print("break",counter)
                if len(collision)==0:
                    self.coin_list.append(coin)
                    break
            #coin = Coin()#arcade.AnimatedTimeSprite(scale=0.3)
            self.all_sprites_list.append(coin)

        # Set the background color
        self.physics_engine = arcade.PhysicsEnginePlatformer(self.player, self.wall_list,gravity_constant=0.5)
        arcade.set_background_color(arcade.color.AMAZON)

    def on_draw(self):
        """
        Render the screen.
        """

        # This command has to happen before we start drawing
        arcade.start_render()

        # Draw all the sprites.
        self.all_sprites_list.draw()
        self.wall_list.draw()

        # Put the text on the screen.
        output = "Score: {}".format(self.score)
        arcade.draw_text(output, 10+self.view_left, 20, arcade.color.WHITE, 14)

    def on_key_press(self, key, modifiers):
        """
        Called whenever the mouse moves.
        """
        self.key_pressed=True
        if key == arcade.key.UP:
            #self.player.change_y = MOVEMENT_SPEED
            #    if key == arcade.key.UP:
            if self.physics_engine.can_jump():
                print("can_jump")
                self.player.change_y = 10
                if self.player.get_last_side()=="left":
                    self.player.set_action("left_jump")
                else:
                    self.player.set_action("right_jump")
            #pass
        elif key == arcade.key.DOWN:
            #self.player.change_y = -MOVEMENT_SPEED
            pass
        elif key == arcade.key.A:
            self.player.change_x=0 # dont move while shooting
            if self.player.get_last_side()=="left":
                self.player.set_action("left_attack")
            else:
                self.player.set_action("right_attack")
        elif key == arcade.key.S:
            self.player.change_x=0 # dont move while shooting
            if self.player.get_last_side()=="left":
                self.player.set_action("left_dead")
            else:
                self.player.set_action("right_dead")
        elif key == arcade.key.LEFT:
            self.player.change_x = -MOVEMENT_SPEED
            self.player.set_action("left_walk")
        elif key == arcade.key.RIGHT:
            self.player.change_x = MOVEMENT_SPEED
            self.player.set_action("right_walk")
        elif key == arcade.key.SPACE:
            print("space ")
            self.player.set_action("right_walk")

    def on_key_release(self, key, modifiers):
        """
        Called when the user presses a mouse button.
        """
        self.key_pressed=False
        if key == arcade.key.UP or key == arcade.key.DOWN:
            self.player.change_y = 0
        elif key == arcade.key.LEFT or key == arcade.key.RIGHT:
            self.player.change_x = 0
            if self.player.get_last_side()=="left":
                self.player.set_action("left_idle")
            else:
                self.player.set_action("right_idle")

    def update(self, delta_time):
        """ Movement and game logic """
        self.all_sprites_list.update()
        self.all_sprites_list.update_animation()
        self.physics_engine.update()

        # Generate a list of all sprites that collided with the player.
        hit_list=detect_collision(self.player,self.coin_list)
        for coin_index in hit_list:
            print(coin_index,len(self.coin_list))
            self.coin_list.remove(coin_index)
            print(coin_index.width)
            coin_index.kill()
            self.score+=1
        zombie_hit=detect_collision(self.player,self.zombie_list)
        for zombie in zombie_hit:
            print("zombie bite",zombie)
            if zombie.get_last_side()=="left":
                zombie.set_action("left_attack")
            else:
                zombie.set_action("right_attack")

        changed = False

        # Scroll left
        left_bndry = self.view_left + VIEWPORT_MARGIN
        if self.player.left < left_bndry:
            self.view_left -= int(left_bndry - self.player.left)
            changed = True

        # Scroll right
        right_bndry = self.view_left + SCREEN_WIDTH - RIGHT_MARGIN
        if self.player.right > right_bndry:
            self.view_left += int(self.player.right - right_bndry)
            changed = True

        # If we need to scroll, go ahead and do it.
        if changed:
            arcade.set_viewport(self.view_left,
                                SCREEN_WIDTH + self.view_left,
                                self.view_bottom,
                                SCREEN_HEIGHT + self.view_bottom)




def main():
    """ Main method """
    window = MyGame(SCREEN_WIDTH, SCREEN_HEIGHT)
    window.setup()
    arcade.run()


if __name__ == "__main__":
    main()