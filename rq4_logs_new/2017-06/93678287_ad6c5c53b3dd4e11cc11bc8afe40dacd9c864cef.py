import pygame
import random
import time
import os
# Define some colors
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
RED = (255, 0, 0)
BLUE = (0, 0, 255)
 
# --- Classes

class Block(pygame.sprite.Sprite):
    """ This class represents the block. """
    def __init__(self, color):
        # Call the parent class (Sprite) constructor
        super().__init__()
 
        self.image = pygame.Surface([20, 15])
        self.image.fill(color)
        self.image = pygame.image.load("alien.png").convert()
        self.image.set_colorkey(BLACK)
        
        
        self.rect = self.image.get_rect()
 
    def reset_pos(self):
        """ Reset position to the top of the screen, at a random x location.
        Called by update() or the main program loop if there is a collision.
        """
        self.rect.y = random.randrange(-300, -20)
        self.rect.x = random.randrange(0, screen_width)
 
    def update(self):
        """ Called each frame. """
 
        # Move block down one pixel
        self.rect.y += 1
        if level == 2:
            self.rect.y +=3
 
        # If block is too far down, reset to top of screen.
        if self.rect.y > 750:
            self.reset_pos()
            
class Player(pygame.sprite.Sprite):
    """ This class represents the Player. """
 
    def __init__(self):
        """ Set up the player on creation. """
        # Call the parent class (Sprite) constructor
        super().__init__()
        
 
        self.image = pygame.image.load("player.png").convert()
     
        # Set our transparent color
        self.image.set_colorkey(WHITE)

 
        self.rect = self.image.get_rect()
 
    def update(self):
        """ Update the player's position. """
        # Get the current mouse position. This returns the position
        # as a list of two numbers.
        pos = pygame.mouse.get_pos()
 
        # Set the player x position to the mouse x position
        self.rect.x = pos[0]
        
        
        
 
 
class Bullet(pygame.sprite.Sprite):
    """ This class represents the bullet . """
    def __init__(self):
        # Call the parent class (Sprite) constructor
        super().__init__()
 
        self.image = pygame.Surface([4, 10])
        self.image.fill(WHITE)
 
        self.rect = self.image.get_rect()
 
    def update(self):
        """ Move the bullet. """
        self.rect.y -= 5
 
def pause():
     test = input("Press Enter to continue...")
# --- Create the window

# Initialize Pygame
pygame.init()
 
# Set the height and width of the screen
screen_width = 700
screen_height = 700
screen = pygame.display.set_mode([screen_width, screen_height])
pygame.display.set_caption("My Game")
background_image = pygame.image.load("background.bmp").convert()
# --- Sprite lists
pygame.mixer.music.load('music.ogg')
pygame.mixer.music.set_endevent(pygame.constants.USEREVENT)

# This is a list of every sprite. All blocks and the player block as well.
all_sprites_list = pygame.sprite.Group()
 
# List of each block in the game
block_list = pygame.sprite.Group()
 
# List of each bullet
bullet_list = pygame.sprite.Group()

player_list = pygame.sprite.Group()
 
# --- Create the sprites
 
for i in range(25):
    # This represents a block
    block = Block(BLUE)
 
    # Set a random location for the block
    block.rect.x = random.randrange(650)
    block.rect.y = random.randrange(-300, -20)
 
    # Add the block to the list of objects
    block_list.add(block)
    all_sprites_list.add(block)
 
# Create a red player block
player = Player()
all_sprites_list.add(player)
 
# Loop until the user clicks the close button.
done = False
gameover = False
# Used to manage how fast the screen updates
clock = pygame.time.Clock()
 
score = 0
level = 1
player.rect.y = 600


font = pygame.font.Font(None, 36)
 
display_instructions = True
instruction_page = 1
name = ""
pygame.mixer.music.play()
click_sound = pygame.mixer.Sound("lazer.ogg")
while not done and display_instructions:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            done = True
        if event.type == pygame.KEYDOWN:
            if event.unicode.isalpha():
                name += event.unicode
            elif event.key == pygame.K_BACKSPACE:
                name = name[:-1]
            elif event.key == pygame.K_RETURN:
                instruction_page += 1  
                if instruction_page == 3:
                    display_instructions = False
                    pygame.mixer.music.stop() 
 
    # Set the screen background
    screen.fill(BLACK)
    screen.blit(background_image, [0, 0])
 
    if instruction_page == 1:
        # Draw instructions, page 1
        # This could also load an image created in another program.
        # That could be both easier and more flexible.
 
        text = font.render("Instructions", True, WHITE)
        screen.blit(text, [10, 10])
       
        text = font.render("Enter your name: ", True, WHITE)
        screen.blit(text, [10, 40])    
       
        text = font.render(name, True, WHITE)
        screen.blit(text, [220, 40])        
 
        text = font.render("Hit enter to continue", True, WHITE)
        screen.blit(text, [10, 70])



    if instruction_page == 2:
        # Draw instructions, page 2
        text = font.render("This game you fight aliens with your mouse", True, WHITE)
        screen.blit(text, [10, 10])    
 
        text = font.render("Shoot with the left mouse button", True, WHITE)
        screen.blit(text, [10, 40])
 
        text = font.render("Move with the mouse on the x axis", True, WHITE)
        screen.blit(text, [10, 70])
 
        text = font.render("Hit enter to continue", True, WHITE)
        screen.blit(text, [10, 100])
        
       
    pygame.display.flip()
     
        # --- Limit to 20 frames per second
    clock.tick(60)
    


while not done:
    # --- Event Processing
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            done = True
 
        elif event.type == pygame.MOUSEBUTTONDOWN:
            # Fire a bullet if the user clicks the mouse button
            click_sound.play()
            bullet = Bullet()
            # Set the bullet so it is where the player is
            bullet.rect.x = player.rect.x
            bullet.rect.y = player.rect.y
            # Add the bullet to the lists
            all_sprites_list.add(bullet)
            bullet_list.add(bullet)
 
    # --- Game logic
 


 
    # Call the update() method on all the sprites
    all_sprites_list.update()
 
    # Calculate mechanics for each bullet
    for bullet in bullet_list:
 
        # See if it hit a block
        block_hit_list = pygame.sprite.spritecollide(bullet, block_list, True)
 
        # For each block hit, remove the bullet and add to the score
        for block in block_hit_list:
            bullet_list.remove(bullet)
            all_sprites_list.remove(bullet)
            score += 1
            print(score)
            if score == 1:
                level += 1
                
                for block in block_list:
                    block.reset_pos()                
                

                          
 
        # Remove the bullet if it flies up off the screen
        if bullet.rect.y < -10:
            bullet_list.remove(bullet)
            all_sprites_list.remove(bullet)
    


        

    # --- Draw a frame
    screen.blit(background_image, [0, 0])
    leveltext = "LEVEL: " + str(level)
    text = font.render(leveltext, True, WHITE)
    screen.blit(text, [0, 580])    
    scoretext = "SCORE: " + str(score)
    text = font.render(scoretext, True, WHITE)
    screen.blit(text, [0, 680])
    
    if pygame.sprite.spritecollide(player, block_list, True):    
        gameover = True


    if gameover == True:
        screen.fill(BLACK)
        text = font.render("GAME OVER", True, WHITE)
        screen.blit(text, [260, 300])
        text = font.render("Press enter to quit", True, WHITE)
        screen.blit(text, [230, 350])    
        all_sprites_list.remove(block)
        block_list.remove(block)
        time.sleep(.1)
          
        if event.type == pygame.KEYDOWN:
            if event.key == pygame.K_RETURN:
                pygame.quit()        
    # Clear the screen

 
    # Draw all the spites
    all_sprites_list.draw(screen)
 
    # Go ahead and update the screen with what we've drawn.
    pygame.display.flip()
 
    # --- Limit to 20 frames per second
    clock.tick(60)
 
pygame.quit()