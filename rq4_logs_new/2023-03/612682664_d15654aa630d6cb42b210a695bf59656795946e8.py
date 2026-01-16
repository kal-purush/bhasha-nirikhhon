# Atari Game in Python

import pygame
from pygame.math import Vector2
import random
# Color Definitions
RED = (255, 0, 0)
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
GREY = (226, 226, 226)
DARK_GREY = (119, 119, 119)
ORANGE = (255, 141, 2)
YELLOW = (255, 242, 2)
MINT_GREEN = (2, 255, 132)
TEAL_BLUE = (2, 238, 255)
BRIGHT_BLUE = (2, 23, 255)
block_color = [ORANGE, YELLOW, MINT_GREEN, TEAL_BLUE, BRIGHT_BLUE]

# Setting Up Essentials
pygame.init()
width = 1295
height = 750
SIZE = (width, height)
SCREEN = pygame.display.set_mode(SIZE)
pygame.display.set_caption("Atari Breakout")
done = False
clock = pygame.time.Clock()

# Ball Class
class Tile(pygame.sprite.Sprite):
    def __init__(self, color, width, height):
        super().__init__()
        self.image = pygame.Surface([width, height])
        self.image.fill(color)
        self.rect = self.image.get_rect()
        #make 1/4 of the tiles powerup
        num = random.randint(1, 4)
        if num == 4:
            self.colide_num = random.randint(1, 3)
        else:
            self.colide_num = 0
 
class Paddle(pygame.sprite.Sprite):
    def __init__(self, x, y):
        super().__init__()
        self.image = pygame.Surface([200, 5])
        self.image.fill(WHITE)
        self.rect = self.image.get_rect()
        self.rect.x = x
        self.rect.y = y
        self.change_x = 0
    def changespeed(self, x):
        self.change_x += x
    def update(self):
        self.rect.x += self.change_x
        if self.rect.x < 40:
            self.rect.x = 40
        if self.rect.x > 1050:
            self.rect.x = 1050

class Ball(pygame.sprite.Sprite):
    def __init__(self, x, y):
        super().__init__()
        self.image = pygame.image.load("Ball.png").convert_alpha()
        self.pos = Vector2(x, y)
        self.rect = self.image.get_rect(center = self.pos)
        self.direction = Vector2(1, -1)
        self.speed = 5
    def tile_reflect(self):
        self.direction = self.direction.reflect(Vector2(0, 10))
    def paddle_reflect(self):
        self.direction = Vector2(self.direction[0], -self.direction[1])
        self.speed += 0.25
    def update(self):
        if self.speed > 7:
            self.speed = 7
        self.pos += self.direction * self.speed
        self.rect.center = self.pos
        if self.pos[0] > 1275 or self.pos[0] < 20:
            self.direction[0] *= -1
        if self.pos[1] > 705 or self.pos[1] < 0:
            self.direction[1] *= -1

class Laser(pygame.sprite.Sprite):
    def __init__(self):
        super().__init__()
        self.image = pygame.Surface([1295, 2.5])
        self.image.fill(RED)
        self.rect = self.image.get_rect()
        self.rect.x = 0
        self.rect.y = 645
def rectangle_box(box_color, box_x, box_y, box_width, box_height, font_size, message, font_color, text_x, text_y):
    pygame.draw.rect(SCREEN, box_color, [box_x, box_y, box_width, box_height])
    font = pygame.font.Font('Games.ttf', font_size)
    text = font.render(message, True, font_color)
    SCREEN.blit(text, [text_x, text_y])
# Creating Sprite List
tile_sprite_list = pygame.sprite.Group()
lose_methods = pygame.sprite.Group()
player = Paddle(540, 640)
ball = Ball(640.5, 626)
laser = Laser()
# All the Sprites
all_sprites_list = pygame.sprite.Group()
all_sprites_list.add(player)
all_sprites_list.add(ball)
all_sprites_list.add(laser)
row_num = 0
while row_num < 5:
    i = 0
    while i < 14:
        tile = Tile(block_color[row_num], 80, 20)
        tile.rect.y = 20 + 55*row_num
        tile.rect.x = 50 + 85*i
        tile_sprite_list.add(tile)
        all_sprites_list.add(tile)
        i += 1
    row_num += 1
lose_methods.add(laser)
# Setting Up Score
score = 0

# Setting Up Sound
paddle_sound = pygame.mixer.Sound("PaddleSound.wav")
tile_collision_sound = pygame.mixer.Sound("TileCollisionSound.wav")

# Main Loop
while not done:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            done = True
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_LEFT:
                player.changespeed(-7.5)
            elif event.key == pygame.K_RIGHT:
                player.changespeed(7.5)
        elif event.type == pygame.KEYUP:
            if event.key == pygame.K_LEFT:
                player.changespeed(7.5)
            elif event.key == pygame.K_RIGHT:
                player.changespeed(-7.5)
  
    # Drawing Code
    SCREEN.fill(BLACK)

    # Setting Up Tile Collisions and Powerups
    tiles_hit_list = pygame.sprite.spritecollide(ball, tile_sprite_list, True)
    for tile in tiles_hit_list:
        ball.tile_reflect()
        tile_collision_sound.play()
        score += 1
    if score == 70:
        ball.speed = 0
        rectangle_box(DARK_GREY, 340, 340, 640, 100, 40, "You Win", RED, 560, 375)

    # Setting Up Laser Collision
    if ball.rect.colliderect(laser):
        ball.speed = 0
        player.changespeed(-7.5)
        rectangle_box(DARK_GREY, 340, 340, 640, 100, 40, "You Lose", RED, 560, 375)
        
    # Setting Up Paddle Collision
    if ball.rect.colliderect(player):
        ball.paddle_reflect()
        paddle_sound.play()
        
    # Setting Up Scoreboard
    rectangle_box(GREY, 0, 645, 1295, 215, 40, 'Score: ' + str(score), RED, 100, 680)
      
    # Handling All the Sprites
    all_sprites_list.update()
    all_sprites_list.draw(SCREEN)
    
    # Screen Update
    pygame.display.flip()
    
    # Frame Rate
    clock.tick(60)

# Closing the Window
pygame.quit()