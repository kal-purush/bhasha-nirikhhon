import pygame

# Global constants
 
# Colors
BLACK = (0, 0, 0)
WHITE = (191, 191, 191)
GREEN = (38, 166, 91)
RED = (255, 0, 0)
BLUE = (137, 196, 244)
CLOUD = (190, 144, 212)
 
# Screen dimensions
SCREEN_WIDTH = 900
SCREEN_HEIGHT = 600

STANDING_R = 0
STANDING_L = 1
JUMPING_R = 2
JUMPING_L = 3

PLAYER_X = 10
PLAYER_Y = SCREEN_HEIGHT - 200

# Set the height and width of the screen
size = [SCREEN_WIDTH, SCREEN_HEIGHT]
screen = pygame.display.set_mode(size)
pygame.display.set_caption("Platformer")

PLAYER_ICON = pygame.image.load("images/player_2/player_jump.png").convert_alpha()

 
class Player(pygame.sprite.Sprite):
    """ This class represents the bar at the bottom that the player
        controls. """
    player_image = []
    walking_frames_right = []
    walking_frames_left = []

    direction = "R"
    walking = False
    jumping = False

    # -- Methods
    def __init__(self):
        """ Constructor function """
        # Call the parent's constructor
        super().__init__()

        standing_right = pygame.image.load("images/player_2/player_stand.png").convert_alpha()
        standing_left = pygame.transform.flip(standing_right, True, False)
        self.player_image.append(standing_right)
        self.player_image.append(standing_left)

        jumping_right = pygame.image.load("images/player_2/player_jump.png").convert_alpha()
        jumping_left = pygame.transform.flip(jumping_right, True, False)
        self.player_image.append(jumping_right)
        self.player_image.append(jumping_left)

        walking_right_1 = pygame.image.load("images/player_2/player_walk1.png").convert_alpha()
        walking_right_2 = pygame.image.load("images/player_2/player_walk2.png").convert_alpha()
        self.walking_frames_right.append(walking_right_1)
        self.walking_frames_right.append(walking_right_2)

        walking_left_1 = pygame.transform.flip(walking_right_1, True, False)
        walking_left_2 = pygame.transform.flip(walking_right_2, True, False)
        self.walking_frames_left.append(walking_left_1)
        self.walking_frames_left.append(walking_left_2)

        self.image = self.player_image[STANDING_R]
 
        # Set a referance to the image rect.
        self.rect = self.image.get_rect()
 
        # Set speed vector of player
        self.change_x = 0
        self.change_y = 0
 
        # List of sprites we can bump against
        self.level = None
 
    def update(self):
        """ Move the player. """
        # Gravity
        self.calc_grav()

        # Move left/right
        self.rect.x += self.change_x
        pos = self.rect.x + self.level.world_shift
        if self.direction == "R" and self.walking == True:
            frame = (pos // 50) % len(self.walking_frames_right)
            self.image = self.walking_frames_right[frame]
        elif self.direction == "L" and self.walking == True:
            frame = (pos // 50) % len(self.walking_frames_left)
            self.image = self.walking_frames_left[frame]

        # See if we hit anything
        block_hit_list = pygame.sprite.spritecollide(self, self.level.platform_list, False)
        for block in block_hit_list:
            # If we are moving right,
            # set our right side to the left side of the item we hit
            if self.change_x > 0:
                self.rect.right = block.rect.left
            elif self.change_x < 0:
                # Otherwise if we are moving left, do the opposite.
                self.rect.left = block.rect.right

        pygame.draw.rect
 
        # Move up/down
        self.rect.y += self.change_y
        if self.direction == "R" and self.jumping == True:
            self.image = self.player_image[JUMPING_R]
        elif self.direction == "L" and self.jumping == True:
            self.image = self.player_image[JUMPING_L]
 
        # Check and see if we hit anything
        block_hit_list = pygame.sprite.spritecollide(self, self.level.platform_list, False)
        for block in block_hit_list:
            # Reset our position based on the top/bottom of the object.
            if self.change_y > 0:
                self.rect.bottom = block.rect.top
            elif self.change_y < 0:
                self.rect.top = block.rect.bottom
            # Stop our vertical movement
            self.change_y = 0
            self.jumping = False
            if self.change_x != 0:
                self.walking = True
            elif self.change_x == 0:
                if self.direction == "R":
                    self.image = self.player_image[STANDING_R]
                else:
                    self.image = self.player_image[STANDING_L]
        
    def calc_grav(self):
        """ Calculate effect of gravity. """
        if self.change_y == 0:
            self.change_y = 1
        else:
            self.change_y += 1
 
    def jump(self):
        """ Called when user hits 'jump' button. """
        self.rect.y += 2
        platform_hit_list = pygame.sprite.spritecollide(self, self.level.platform_list, False)
        self.rect.y -= 2
 
        # If it is ok to jump, set our speed upwards
        if len(platform_hit_list) > 0: # or self.rect.bottom >= SCREEN_HEIGHT:
            self.change_y = -20    
            self.walking = False
            self.jumping = True

    def stop_jump(self):
        self.jumping = False
        if self.direction == "R":
            self.image = self.player_image[STANDING_R]
        else: 
            self.image = self.player_image[STANDING_L]
        
    # Player-controlled movement:
    def go_left(self):
        """ Called when the user hits the left arrow. """
        self.change_x = -6
        self.direction = "L"
        self.walking = True
 
    def go_right(self):
        """ Called when the user hits the right arrow. """
        self.change_x = 6
        self.direction = "R"
        self.walking = True
 
    def stop(self):
        """ Called when the user lets off the keyboard. """
        self.change_x = 0
        self.walking = False
        self.jumping = False
        if self.direction == "R":
            self.image = self.player_image[STANDING_R]
        else:
            self.image = self.player_image[STANDING_L]

class EnemyBlob(pygame.sprite.Sprite):

    moving_frames_right = []
    moving_frames_left = []

    def __init__(self, start, end, height, scale):
        blob_image_1 = pygame.image.load("images/enemies/slimeBlue.png").convert_alpha()
        blob_width = blob_image_1.get_width()
        blob_height = blob_image_1.get_height()
        blob_image_1 = pygame.transform.scale(blob_image_1, (int(blob_width * scale), int(blob_height * scale)))
    
        blob_image_2 = pygame.image.load("images/enemies/slimeBlue_move.png").convert_alpha()
        blob_width = blob_image_2.get_width()
        blob_height = blob_image_2.get_height()
        blob_image_2 = pygame.transform.scale(blob_image_2, (int(blob_width * scale), int(blob_height * scale)))

        self.moving_frames_left.append(blob_image_1)
        self.moving_frames_left.append(blob_image_2)

        move_right_1 = pygame.transform.flip(blob_image_1, True, False)
        move_right_2 = pygame.transform.flip(blob_image_2, True, False)
        self.moving_frames_right.append(move_right_1)
        self.moving_frames_right.append(move_right_2)

        self.height = height

        super().__init__()
        self.image = blob_image_1
        self.rect = self.image.get_rect()
        self.direction = "R"
        self.path = [start, end]
        self.change_x = 3
        self.rect.y = height
        self.rect.x = start
        
    def update(self):
        self.move()
        pos = self.rect.x - self.level.world_shift
        if self.direction == "R":
            frame = (pos // 50) % len(self.moving_frames_right)
            self.image = self.moving_frames_right[frame]
        elif self.direction == "L":
            frame = (pos // 50) % len(self.moving_frames_left)
            self.image = self.moving_frames_left[frame]

    def move(self):
        cur_pos = self.rect.x - self.level.world_shift
        if self.change_x > 0:
            if cur_pos + self.change_x < self.path[1]:
                self.rect.x += self.change_x
            else:
                self.change_x = self.change_x * -1
                self.direction = "L"
        else:
            if cur_pos - self.change_x > self.path[0]:
                self.rect.x += self.change_x
            else:
                self.change_x = self.change_x * -1
                self.direction = "R"

class EnemyFrog(pygame.sprite.Sprite):

    moving_frames_right = []
    moving_frames_left = []

    def __init__(self, start, direction, height, scale):
        frog_image_1 = pygame.image.load("images/enemies/frog.png").convert_alpha()
        frog_width = frog_image_1.get_width()
        frog_height = frog_image_1.get_height()
        frog_image_1 = pygame.transform.scale(frog_image_1, (int(frog_width * scale), int(frog_height * scale)))
    
        frog_image_2 = pygame.image.load("images/enemies/frog_move.png").convert_alpha()
        frog_width = frog_image_2.get_width()
        frog_height = frog_image_2.get_height()
        frog_image_2 = pygame.transform.scale(frog_image_2, (int(frog_width * scale), int(frog_height * scale)))

        self.moving_frames_left.append(frog_image_1)
        self.moving_frames_left.append(frog_image_2)

        move_right_1 = pygame.transform.flip(frog_image_1, True, False)
        move_right_2 = pygame.transform.flip(frog_image_2, True, False)
        self.moving_frames_right.append(move_right_1)
        self.moving_frames_right.append(move_right_2)

        self.height = height
        self.start = start

        super().__init__()
        self.image = frog_image_1
        self.rect = self.image.get_rect()
        self.direction = direction
        self.change_x = 2
        self.change_y = 0
        self.rect.y = height
        self.rect.x = start
        if self.direction == "R":
            self.right = True
        
    def update(self):
        self.calc_grav()
        self.move()

        self.rect.y += self.change_y

        block_hit_list = pygame.sprite.spritecollide(self, self.level.platform_list, False)
        for block in block_hit_list:
            # Reset our position based on the top/bottom of the object.
            if self.change_y > 0:
                self.rect.bottom = block.rect.top
            elif self.change_y < 0:
                self.rect.top = block.rect.bottom
            # Stop our vertical movement
            self.change_y = 0

        pos = self.rect.x - self.level.world_shift
        if self.direction == "R":
            frame = (pos // 20) % len(self.moving_frames_right)
            self.image = self.moving_frames_right[frame]
        elif self.direction == "L":
            frame = (pos // 20) % len(self.moving_frames_left)
            self.image = self.moving_frames_left[frame]

        block_hit_list = pygame.sprite.spritecollide(self, self.level.platform_list, False)
        for block in block_hit_list:
            if self.change_x > 0:
                self.direction = "L" 
            elif self.change_x < 0:
                self.direction = "R" 

        if self.rect.y > SCREEN_HEIGHT:
            self.rect.y = self.height
            self.rect.x = self.start + self.level.world_shift
            if self.right == True:
                self.direction = "R"
            else:
                self.direction = "L"

    def calc_grav(self):
        """ Calculate effect of gravity. """
        if self.change_y == 0:
            self.change_y = 1
        else:
            self.change_y += 1

    def move(self):
        if self.direction == "L":
            self.rect.x -= self.change_x     
        else:
            self.rect.x += self.change_x 
             
 
class Platform(pygame.sprite.Sprite):
    def __init__(self, width, height):
        super().__init__()
        self.image = pygame.Surface([width, height])
        self.image.fill(GREEN)
        self.rect = self.image.get_rect()

class PlatformImage(pygame.sprite.Sprite):
    def __init__(self, platform_image, scale):
        super().__init__()
        picture = pygame.image.load(platform_image).convert_alpha()
        width = picture.get_width()
        height = picture.get_height()
        picture = pygame.transform.scale(picture, (int(width * scale), int(height * scale)))
        self.image = picture
        self.rect = self.image.get_rect()

class MovingPlatform(PlatformImage):
    """ This is a fancier platform that can actually move. """
    change_x = 0
    change_y = 0

    boundary_top = 0
    boundary_bottom = 0
    boundary_left = 0
    boundary_right = 0

    level = None
    player = None

    def update(self):
        """ Move the platform.
            If the player is in the way, it will shove the player
            out of the way. This does NOT handle what happens if a
            platform shoves a player into another object. Make sure
            moving platforms have clearance to push the player around
            or add code to handle what happens if they don't. """

        # Move left/right
        self.rect.x += self.change_x
        # See if we hit the player
        hit = pygame.sprite.collide_rect(self, self.player)
        if hit:
            # We did hit the player. Shove the player around and
            # assume he/she won't hit anything else.

            # If we are moving right, set our right side
            # to the left side of the item we hit
            if self.change_x < 0:
                self.player.rect.right = self.rect.left
            else:
                # Otherwise if we are moving left, do the opposite.
                self.player.rect.left = self.rect.right

        # Move up/down
        self.rect.y += self.change_y

        # Check and see if we the player
        hit = pygame.sprite.collide_rect(self, self.player)
        if hit:
            # We did hit the player. Shove the player around and
            # assume he/she won't hit anything else.

            # Reset our position based on the top/bottom of the object.
            if self.change_y < 0:
                self.player.rect.bottom = self.rect.top
            else:
                self.player.rect.top = self.rect.bottom

        # Check the boundaries and see if we need to reverse
        # direction.
        if self.rect.bottom > self.boundary_bottom or self.rect.top < self.boundary_top:
            self.change_y *= -1

        cur_pos = self.rect.x - self.level.world_shift
        if cur_pos < self.boundary_left or cur_pos > self.boundary_right:
            self.change_x *= -1

class Coin(pygame.sprite.Sprite):
    def __init__(self, coin_image, scale):
        super().__init__()
        picture = pygame.image.load(coin_image).convert_alpha()
        width = picture.get_width()
        height = picture.get_height()
        picture = pygame.transform.scale(picture, (int(width * scale), int(height * scale)))
        self.image = picture
        self.rect = self.image.get_rect()

class Scenery(pygame.sprite.Sprite):
    def __init__(self, scenery_image, scale):
        super().__init__()
        picture = pygame.image.load(scenery_image).convert_alpha()
        width = picture.get_width()
        height = picture.get_height()
        picture = pygame.transform.scale(picture, (int(width * scale), int(height * scale)))
        self.image = picture
        self.rect = self.image.get_rect()

class Trophy(pygame.sprite.Sprite):
    def __init__(self, trophy_image, scale):
        super().__init__()
        picture = pygame.image.load(trophy_image).convert_alpha()
        width = picture.get_width()
        height = picture.get_height()
        picture = pygame.transform.scale(picture, (int(width * scale), int(height * scale)))
        self.image = picture
        self.rect = self.image.get_rect()

class Liquid(pygame.sprite.Sprite):
    def __init__(self, liquid_image, x, y, scale):
        super().__init__()
        picture = pygame.image.load(liquid_image).convert_alpha()
        width = picture.get_width()
        height = picture.get_height()
        picture = pygame.transform.scale(picture, (int(width * scale), int(height * scale)))
        self.image = picture
        self.rect = self.image.get_rect()
        self.vel_x = 1
        self.vel_y = 0.2
        self.rect.y = y
        self.pos_y = y
        self.rect.x = x
        self.path_x = [x, x + 150]
        self.path_y = [y, y + 10]


    def update(self):
        cur_pos_x = self.rect.x - self.level.world_shift
        if self.vel_x > 0:
            if cur_pos_x + self.vel_x < self.path_x[1]:
                self.rect.x += self.vel_x
            else:
                self.vel_x = self.vel_x * -1
        else:
            if cur_pos_x - self.vel_x > self.path_x[0]:
                self.rect.x += self.vel_x
            else:
                self.vel_x = self.vel_x * -1

        if self.vel_y > 0:
            if self.pos_y + self.vel_y < self.path_y[1]:
                self.pos_y += self.vel_y
            else:
                self.vel_y = self.vel_y * -1
        else:
            if self.pos_y - self.vel_y > self.path_y[0]:
                self.pos_y += self.vel_y
            else:
                self.vel_y = self.vel_y * -1
        self.rect.y = self.pos_y
 
class Level(object):

    world_shift = 0
  
    def __init__(self, player):
        """ Constructor. Pass in a handle to player. Needed for when moving platforms
            collide with the player. """
        self.background_scenery_list = pygame.sprite.Group()
        self.behind_scenery_list = pygame.sprite.Group()
        self.hazard_list = pygame.sprite.Group()
        self.platform_list = pygame.sprite.Group()
        self.enemy_list = pygame.sprite.Group()
        self.coin_list = pygame.sprite.Group()
        self.trophy_list = pygame.sprite.Group()
        self.scenery_list = pygame.sprite.Group()
        self.player = player
         
        # Background image
        self.background = None
 
    # Update everythig on this level
    def update(self):
        """ Update everything in this level."""
        self.background_scenery_list.update()
        self.behind_scenery_list.update()
        self.hazard_list.update()
        self.platform_list.update()
        self.enemy_list.update()  
        self.coin_list.update()
        self.trophy_list.update()
        self.scenery_list.update()
 
    def draw(self, screen):
        """ Draw everything on this level. """
 
        # Draw the background
        #screen.fill(BLUE)
        screen.fill(self.screen_colour)
 
        # Draw all the sprite lists that we have
        self.background_scenery_list.draw(screen)
        self.behind_scenery_list.draw(screen)
        self.hazard_list.draw(screen)
        self.platform_list.draw(screen)
        self.enemy_list.draw(screen)
        self.coin_list.draw(screen)
        self.trophy_list.draw(screen)
        self.scenery_list.draw(screen)

    def shift_world(self, shift_x):
        """ When the user moves left/right and we need to scroll everything: """
        self.world_shift += shift_x

        for platform in self.platform_list:
            platform.rect.x += shift_x
        
        for enemy in self.enemy_list:
            enemy.rect.x += shift_x

        for hazard in self.hazard_list:
            hazard.rect.x += shift_x
        
        for coin in self.coin_list:
            coin.rect.x += shift_x

        for trophy in self.trophy_list:
            trophy.rect.x += shift_x

        for scenery in self.scenery_list:
            scenery.rect.x += shift_x

        for scenery in self.behind_scenery_list:
            scenery.rect.x += shift_x

        for scenery in self.background_scenery_list:
            scenery.rect.x += shift_x / 2

# Create platforms for the level
class Level_01(Level):
    """ Definition for level 1. """
    def __init__(self, player):
        """ Create level 1. """
        Level.__init__(self, player)

        self.level_coins = 3

        self.screen_colour = CLOUD

        self.level_exit_left = 1290
        self.level_exit_right = 1310

        start = -1000
        end = 2500
        for number in range(start, end):
            if number % 64 == 0:
                block = Liquid("images/tiles/lavaTop_low.png",number, 550, 0.5)
                block.level = self
                block.player = self.player
                self.hazard_list.add(block)

        dirt_platforms = [[0, 15, 536, 0.5],
                          [1200, 10, 536, 0.5],
                          [200, 4, 175, 0.5],
                          [600, 3, 175, 0.5],
                          [1500, 4, 350, 0.5],
                          [1176, 3, 175, 0.5]
                          ]      
        
        for dirt_platform in dirt_platforms:
            start = dirt_platform[0]
            count = dirt_platform[1] - 1
            height = dirt_platform[2]
            scale = dirt_platform[3]
            for number in range((start + ((count) * 64)) - (start + 64)):
                if number % 64 == 0:
                    block = PlatformImage("images/tiles/dirtMid.png", scale)
                    block.rect.x = number + (start + 64)
                    block.rect.y = height
                    block.player = self.player
                    self.platform_list.add(block)
                block = PlatformImage("images/tiles/dirtLeft.png", scale)
                block.rect.x = start
                block.rect.y = height
                block.player = self.player
                self.platform_list.add(block)
                block = PlatformImage("images/tiles/dirtRight.png", scale)
                block.rect.x = start + (count * 64)
                block.rect.y = height
                block.player = self.player
                self.platform_list.add(block)

        box = PlatformImage("images/tiles/boxCrate_double.png", 0.5)
        box.rect.x = 800
        box.rect.y = 472 
        box.player = self.player
        self.platform_list.add(box)

        box = PlatformImage("images/tiles/boxCrate_double.png", 0.5)
        box.rect.x = 1550
        box.rect.y = 472
        box.player = self.player
        self.platform_list.add(box)

        bush = Scenery("images/tiles/bush.png", 0.5)
        bush.rect.x = 675
        bush.rect.y = 111
        bush.player = self.player
        self.scenery_list.add(bush)

        level_coins = [[200, 100],
                      [1666, 472],
                      [850, 111]
                      ]

        for collectable in level_coins:
            coin = Coin("images/items/coinGold.png", 0.5)
            coin.rect.x = collectable[0]
            coin.rect.y = collectable[1]
            coin.player = self.player
            self.coin_list.add(coin)

        bridge_platform = [728, 6, 175, 0.5] 
        start = bridge_platform[0]
        count = bridge_platform[1]
        height = bridge_platform[2]
        scale = bridge_platform[3]
        for number in range((start + ((count) * 64)) - (start)):
            if number % 64 == 0:
                bridge = PlatformImage("images/tiles/bridgeA.png", scale)
                bridge.rect.x = number + (start + 64)
                bridge.rect.y = height
                bridge.player = self.player
                self.platform_list.add(bridge)

        door_m = Scenery("images/tiles/doorClosed_mid.png", 0.5)
        door_m.rect.x = 1300
        door_m.rect.y = 472
        self.behind_scenery_list.add(door_m)

        door_t = Scenery("images/tiles/doorClosed_top.png", 0.5)
        door_t.rect.x = 1300
        door_t.rect.y = 408
        self.behind_scenery_list.add(door_t)

        blue_blob = EnemyBlob(200, 736, 472, 0.5)
        blue_blob.player = self.player
        blue_blob.level = self
        self.enemy_list.add(blue_blob)

        level_mountains = [[-100, 300],
                           [1000, 300]
                           ]
        for mountain in level_mountains:
            mountain_image = Scenery("images/MountainsBackground.png", 0.5)
            mountain_image.rect.x = mountain[0]
            mountain_image.rect.y = mountain[1]
            self.background_scenery_list.add(mountain_image)

        frog = EnemyFrog(675, "R", 111, 0.5)
        frog.player = self.player
        frog.level = self
        self.enemy_list.add(frog)

class Level_02(Level):
    """ Definition for level 1. """
    def __init__(self, player):
        """ Create level 1. """
        Level.__init__(self, player)

        self.level_coins = 0

        self.screen_colour = BLUE

        self.level_exit_left = 2000
        self.level_exit_right = 2064

        ship = Liquid("images/ship.png", 1400, 430, 0.2)
        ship.level = self
        ship.player = self.player
        self.scenery_list.add(ship)

        start = -1000
        end = 3000
        for number in range(start, end):
            if number % 64 == 0:
                block = Liquid("images/tiles/waterTop_low.png",number ,550, 0.5)
                block.level = self
                block.player = self.player
                self.scenery_list.add(block)

        grass_platforms = [[-100, 10, 536, 0.5],
                           [344, 5, 350, 0.5],
                           [500, 3, 164, 0.5],
                           [900, 7, 536, 0.5]
                           ]
        
        for grass_platform in grass_platforms:
            start = grass_platform[0]
            count = grass_platform[1] - 1
            height = grass_platform[2]
            scale = grass_platform[3]
            for number in range((start + (count * 64)) - (start + 64)):
                if number % 64 == 0:
                    block = PlatformImage("images/tiles/grassMid.png", scale)
                    block.rect.x = number + (start + 64)
                    block.rect.y = height
                    block.player = self.player
                    self.platform_list.add(block)
                block = PlatformImage("images/tiles/grassCliff_left.png", scale)
                block.rect.x = start
                block.rect.y = height
                block.player = self.player
                self.platform_list.add(block)
                block = PlatformImage("images/tiles/grassCliff_right.png", scale)
                block.rect.x = start + (count * 64)
                block.rect.y = height
                block.player = self.player
                self.platform_list.add(block)

        sign = Scenery("images/tiles/signRight.png", 0.5)
        sign.rect.x = 564
        sign.rect.y = 100
        self.scenery_list.add(sign)

        star = Trophy("images/items/star.png", 0.5)
        star.rect.x = 1088
        star.rect.y = 100
        star.player = self.player
        self.trophy_list.add(star)

        blue_blob = EnemyBlob(344, 600, 286, 0.5)
        blue_blob.player = self.player
        blue_blob.level = self
        self.enemy_list.add(blue_blob)

        clouds = [[0, 100, 1],
                  [400, 275, 0.5],
                  [700, 150, 0.75]
                  ]

        for cloud in clouds:
            cloud_image = Scenery("images/cloud.png", cloud[2])
            cloud_image.rect.x = cloud[0]
            cloud_image.rect.y = cloud[1]
            self.background_scenery_list.add(cloud_image)

        block = MovingPlatform("images/tiles/planet.png", 0.5)
        block.rect.x = 1088
        block.rect.y = 300
        block.boundary_top = 200
        block.boundary_bottom = 400
        block.change_y = 1
        block.player = self.player
        block.level = self
        self.platform_list.add(block)

        box = PlatformImage("images/tiles/boxCrate_double.png", 0.5)
        box.rect.x = 1275
        box.rect.y = 472
        box.player = self.player
        self.platform_list.add(box)
            
def main():
    """ Main Program """
    pygame.init()
 
    # Loop until the user clicks the close button.
    done = False
 
    # Used to manage how fast the screen updates
    clock = pygame.time.Clock()

    game_over = True
    game_win = False

    score = 0
    level_score = 0

    def show_go_screen():
        screen.fill(BLUE)
        font = pygame.font.Font(None,64)
        font2 = pygame.font.Font(None,24)
        
        title = font.render("Platformer", 1, (BLACK))
        title_width = title.get_width()
        
        start = font2.render("Press the space key to start (I'm a little slow)", 1, (BLACK))
        start_width = start.get_width()
        
        score_text = font.render("Score: " + str(score), 1, BLACK)
        score_text_width = score_text.get_width()

        icon_width = PLAYER_ICON.get_width()
        icon_height = PLAYER_ICON.get_height()
        icon_scale = 2
        go_player_icon = pygame.transform.scale(PLAYER_ICON, (int(icon_width * icon_scale), int(icon_height * icon_scale)))
        go_player_icon_width = go_player_icon.get_width()

        screen.blit(go_player_icon, ((SCREEN_WIDTH / 2) - (go_player_icon_width / 2), SCREEN_HEIGHT / 3))
        screen.blit(title, ((SCREEN_WIDTH / 2) - (title_width / 2), SCREEN_HEIGHT / 4))
        screen.blit(start, ((SCREEN_WIDTH / 2) - (start_width / 2), SCREEN_HEIGHT * 3/4))
        if score > 0:
            screen.blit(score_text,((SCREEN_WIDTH / 2) - (score_text_width / 2), SCREEN_HEIGHT * 4/5))

        #draw_text(screen, "Drive", 64, SCREENWIDTH / 2, SCREENHEIGHT / 4)
        #draw_text(screen, "Press a key to begin", 18, SCREENWIDTH / 2, SCREENHEIGHT * 3/4)
        pygame.display.flip()
        waiting = True
        while waiting:
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    pygame.display.quit()
                    pygame.quit()
                    exit()
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_SPACE:
                        waiting = False

    def show_win_screen():
        screen.fill(BLUE)
        font = pygame.font.Font(None,64)
        font2 = pygame.font.Font(None,100)

        title = font.render("Platformer", 1, (BLACK))
        title_width = title.get_width()

        start = font2.render("You Win", 1, (BLACK))
        start_width = start.get_width()

        score_text = font.render("Score: " + str(score), 1, BLACK)
        score_text_width = score_text.get_width()

        screen.blit(title, ((SCREEN_WIDTH / 2) - (title_width / 2), SCREEN_HEIGHT / 4))
        screen.blit(start, ((SCREEN_WIDTH / 2) - (start_width / 2), SCREEN_HEIGHT / 2))
        if score > 0:
            screen.blit(score_text,((SCREEN_WIDTH / 2) - (score_text_width / 2), SCREEN_HEIGHT * 4/5))

        pygame.display.flip()
        waiting = True
        while waiting:
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    pygame.display.quit()
                    pygame.quit()
                    exit()
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_SPACE:
                        waiting = False
        
    # -------- Main Program Loop -----------
    while not done:
        if game_over == True:
            show_go_screen()
            game_over = False
        
            score = 0
            level_score = 0
            player_lives = 3

            # Create the player
            player = Player()
         
            # Create all the levels
            level_list = []
            level_list.append( Level_01(player) )
            level_list.append( Level_02(player) )
         
            # Set the current level
            current_level_no = 0
            current_level = level_list[current_level_no]
             
            active_sprite_list = pygame.sprite.Group()
            player.level = current_level
             
            player.rect.x = PLAYER_X
            player.rect.y = PLAYER_Y
            active_sprite_list.add(player)

        if game_win == True:
            show_win_screen()
            game_win = False

            score = 0
            level_score = 0
            player_lives = 3

            # Create the player
            player = Player()
         
            # Create all the levels
            level_list = []
            level_list.append( Level_01(player) )
            level_list.append( Level_02(player) )
         
            # Set the current level
            current_level_no = 0
            current_level = level_list[current_level_no]
             
            active_sprite_list = pygame.sprite.Group()
            player.level = current_level
             
            player.rect.x = PLAYER_X
            player.rect.y = PLAYER_Y
            active_sprite_list.add(player)

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                done = True
 
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_LEFT and not event.key == pygame.K_RIGHT:
                    player.go_left()
                if event.key == pygame.K_RIGHT and not event.key == pygame.K_LEFT:
                    player.go_right()
                if event.key == pygame.K_UP or event.key == pygame.K_SPACE:
                    player.jump()
 
            if event.type == pygame.KEYUP:
                if event.key == pygame.K_LEFT and player.change_x < 0:
                    player.stop()
                if event.key == pygame.K_RIGHT and player.change_x > 0:
                    player.stop()
                #if event.key == pygame.K_UP or event.key == pygame.K_SPACE:                           
                 #   player.stop_jump()
 
        # Update the player.
        active_sprite_list.update()
 
        # Update items in the level
        current_level.update()

        level_position = 0

        if player_lives < 1:
            game_over = True

        if player.rect.x >= 550:
            diff = player.rect.x - 550
            player.rect.x = 550
            current_level.shift_world(-diff)

        # If the player gets near the left side, shift the world right (+x)
        if player.rect.x <= 200:
            diff = 200 - player.rect.x
            player.rect.x = 200
            current_level.shift_world(diff)

        coin_hit_list = pygame.sprite.spritecollide(player, player.level.coin_list, False, pygame.sprite.collide_rect_ratio(0.5))
        for coin in coin_hit_list:
            score += 1
            level_score += 1
            coin.kill()

        trophy_hit_list = pygame.sprite.spritecollide(player, player.level.trophy_list, False, pygame.sprite.collide_rect_ratio(0.5))
        for trophy in trophy_hit_list:
            game_win = True

        enemy_hit_list = pygame.sprite.spritecollide(player, player.level.enemy_list, False, pygame.sprite.collide_rect_ratio(0.75))
        for enemy in enemy_hit_list:
            player_lives -= 1
            player.rect.x = PLAYER_X + current_level.world_shift
            player.rect.y = PLAYER_Y

        hazard_hit_list = pygame.sprite.spritecollide(player, player.level.hazard_list, False, pygame.sprite.collide_rect_ratio(0.5))
        for hazard in hazard_hit_list:
            player_lives -= 1
            player.rect.x = PLAYER_X + current_level.world_shift
            player.rect.y = PLAYER_Y

        if player.rect.y >= SCREEN_HEIGHT:
            player_lives -= 1
            player.rect.x = PLAYER_X + current_level.world_shift
            player.rect.y = PLAYER_Y

        current_position = player.rect.x - current_level.world_shift 
        if current_position > current_level.level_exit_left and\
           current_position < current_level.level_exit_right and\
           player.rect.y > 408 and\
           level_score >= current_level.level_coins:
            #game_over = True
            player.rect.x = 10 + current_level.world_shift
            player.rect.y = SCREEN_HEIGHT - player.rect.height - 20
            player.rect.x = 10
            if current_level_no < len(level_list)-1:
                current_level_no += 1
                current_level = level_list[current_level_no]
                player.level = current_level
                level_score = 0

        # ALL CODE TO DRAW SHOULD GO BELOW THIS COMMENT
        current_level.draw(screen)
        active_sprite_list.draw(screen)

        font = pygame.font.Font(None,30)
        score_text = font.render("Score: " + str(score), 1, (255, 255, 255))
        screen.blit(score_text, (20, 20))
        cp_text = font.render("Lives left: " + str(player_lives), 1, (255, 255, 255))
        screen.blit(cp_text, (750, 20))
        # ALL CODE TO DRAW SHOULD GO ABOVE THIS COMMENT
 
        # Limit to 60 frames per second
        clock.tick(60)

        # Go ahead and update the screen with what we've drawn.
        pygame.display.flip()
 
    # Be IDLE friendly. If you forget this line, the program will 'hang'
    # on exit.
    pygame.quit()
 
if __name__ == "__main__":
    main()