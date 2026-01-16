import pygame,math,random
from pygame.locals import *
from survival_game_main_workings import *
import survival_game_main_workings

small_enemie1_raw = pygame.transform.scale(pygame.image.load("assets/small_zombie1.png"), (100, 100))
small_enemie2_raw = pygame.transform.scale(pygame.image.load("assets/small_zombie2.png"), (100, 100))
small_enemie3_raw = pygame.transform.scale(pygame.image.load("assets/small_zombie3.png"), (100, 100))
small_enemie4_raw = pygame.transform.scale(pygame.image.load("assets/small_zombie4.png"), (100, 100))

medium_enemy1_raw = pygame.transform.scale(pygame.image.load("assets/medium_zombie1.png"), (100, 100))
medium_enemy2_raw = pygame.transform.scale(pygame.image.load("assets/medium_zombie2.png"), (100, 100))

# list of enemies
small_zombies_list = [small_enemie1_raw,small_enemie2_raw,small_enemie3_raw,small_enemie4_raw]
medium_zombies_list = [medium_enemy1_raw,medium_enemy2_raw]
large_zombies_list = []

MAN_WIDTH, MAN_HEIGHT = 200, 200

man_image1 = pygame.transform.scale(man_image_raw1, (MAN_WIDTH, MAN_HEIGHT))

WIDTH, HEIGHT = 700, 700
win = pygame.display.set_mode((WIDTH, HEIGHT))

player_rect = man_image1.get_rect(center=(WIDTH // 2, HEIGHT // 2))

player_speed = 0  # Reduce player speed
zombie_speed = 0  # Reduce zombie speed


# FONTS CAN BE USED IN THE CODE
pygame.font.init()
MAIN_FRAME_FONT = pygame.font.SysFont('Arial', 30)
ABOUT_GAME_FONT = pygame.font.SysFont('Arial', 10)

# MAIN LOGO IMAGE
# PREFER DEFINING IN ALL GAME BY YOUR OWN
LOGO_IMAGE = "assets/logo1.png"

# SCALING PREFER DEFINING BY OWN IN ALL GAMES
# SET BY DEFAULT OF SCREEN SIZE
LOGO_SCALE_WIDTH, LOGO_SCALE_HEIGHT = 390, 250
LOGO = pygame.transform.scale(pygame.image.load(LOGO_IMAGE), (LOGO_SCALE_WIDTH, LOGO_SCALE_HEIGHT))

BUTTON_HOVER_COLOR = RED
BUTTON_COLOR = (0, 255, 0,0)

LOGO_BACKGROUND_COLOUR = GREEN  # DEFINE AT IN ALL GAME

zombies_list=[]

# LOGO COORDINATES PREFER DEF AT ALL IN YOUR CODE
LOGO_X, LOGO_Y = WIDTH/2 - LOGO.get_width() / 2, HEIGHT/2 - LOGO.get_height()

COLOUR = BLACK
def LOGO_other_elements():
    # Load player image
    player_rect = man_image1.get_rect(center=(WIDTH // 2, HEIGHT // 2))
    player_speed = 5  # Adjust player speed as needed

    # Create a list to store zombie rectangles
    zombies_list = []

    # Load zombie images and create rectangles
    small_zombies_list = [small_enemie1_raw, small_enemie2_raw, small_enemie3_raw, small_enemie4_raw]
    medium_zombies_list = [medium_enemy1_raw, medium_enemy2_raw]

    for zombie_image in small_zombies_list:
        zombie_rect = zombie_image.get_rect(center=(random.randint(50, WIDTH - 50), random.randint(50, HEIGHT - 50)))
        zombies_list.append(zombie_rect)

    for zombie_image in medium_zombies_list:
        zombie_rect = zombie_image.get_rect(center=(random.randint(50, WIDTH - 50), random.randint(50, HEIGHT - 50)))
        zombies_list.append(zombie_rect)

    # Set up your player character and handle player actions (firing, movement)
    nearest_zombie = find_nearest_zombie(player_rect, zombies_list)
    rotated_player, new_player_rect = rotate_player_towards_nearest_zombie(player_rect, nearest_zombie)
    
    # Update the game loop to handle player actions and zombie movements
    move_zombies_towards_player(player_rect , zombies_list, zombie_speed)

    win.blit(rotated_player, new_player_rect)

    # for zombie_rect in zombies_list:
    #     win.blit(zombie_image, zombie_rect)

    for zombie_rect in zombies_list:
        win.blit(zombie_image, zombie_rect)  # Change 'zombie_image' to 'zombie_rect'

def logo_window():
    win.fill(LOGO_BACKGROUND_COLOUR)
    LOGO_other_elements()
    win.blit(LOGO, (LOGO_X, LOGO_Y))

    # Define button dimensions
    BUTTON_WIDTH = 10
    BUTTON_HEIGHT = 30

    # Create a function to draw buttons
    def draw_button(x, y, text, action):
        button_rect = pygame.Rect(x, y, BUTTON_WIDTH, BUTTON_HEIGHT)
        mouse_pos = pygame.mouse.get_pos()
        click = pygame.mouse.get_pressed()

        if button_rect.collidepoint(mouse_pos):
            pygame.draw.rect(win, BUTTON_HOVER_COLOR, button_rect)
            if click[0] == 1:
                if action == "start":
                    # Added code to start the game
                    start_game()

                elif action == "about":
                    # Add code to show about information gemeral_main_frameproduced and all
                    show_about()

                elif action == "quit":
                    pygame.quit()
                    quit()
                elif action == "settings":
                    # Add code to open game settings
                    open_settings()
        else:
            pygame.draw.rect(win, BUTTON_COLOR, button_rect)

        # Render button text
        text_surface = MAIN_FRAME_FONT.render(text, True,(0,0,0,0))
        text_rect = text_surface.get_rect(center=(x + BUTTON_WIDTH // 2, y + BUTTON_HEIGHT // 2))
        win.blit(text_surface, text_rect)

    # Draw buttons
    draw_button(350,350 , "Start", "start")
    draw_button(350,400 , "Settings", "settings")
    draw_button(350,450 , "About", "about")
    draw_button(350,500 , "Quit", "quit")

    pygame.display.update()

class Button:
    def __init__(self, rect, text, action):
        self.rect = rect
        self.text = text
        self.action = action

    # def draw(self, surface):
    #     # Render and draw the button on the specified surface
    #     # You would use self.rect to position and self.text for the button label
    #     pass

buttons = [
    Button(pygame.Rect(50, 100, 200, 50), "Start", "start"),
    Button(pygame.Rect(50, 200, 200, 50), "Settings", "settings"),
    Button(pygame.Rect(50, 300, 200, 50), "About", "about"),
    Button(pygame.Rect(50, 400, 200, 50), "Quit", "quit")

    # Add more buttons as needed
]

def play_pause_buttons(mouse_pos):
    global current_image,play_image,pause_image
    # Display the current image (play or pause)


    win.blit(current_image, (50, 100))

    image_rect = pygame.Rect(50, 100, play_image.get_width(), play_image.get_height())
    if image_rect.collidepoint(mouse_pos):
        if current_image == play_image:
            current_image = pause_image
        else:
            current_image = play_image

def handle_mouse_click(key_pressed):
    # Handle mouse click events here
    # You can check which button was clicked and perform corresponding actions
    # for button in buttons:
    #     if button.rect.collidepoint(mouse_pos):
    #         if button.action == "start":
    #             start_game()
    #         elif button.action == "about":
    #             show_about()
    #         elif button.action == "quit":
    #             run = False
    #         elif button.action == "settings":
    #             open_settings()
    for button in buttons:
        if key_pressed[K_s]:
            start_game()
        if key_pressed[K_a]:
            show_about()
        if key_pressed[K_q]:
            run = False
        if key_pressed[K_t]:
            open_settings()
      
        
# Function to calculate the distance between two points (player and zombie)
def distance(point1, point2):
    x1, y1 = point1
    x2, y2 = point2
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5

# Function to find the nearest zombie
def find_nearest_zombie(player_rect, zombies_list):
    nearest_zombie = None
    min_distance = float('inf')

    for zombie_rect in zombies_list:
        d = distance(player_rect.center, zombie_rect.center)
        if d < min_distance:
            min_distance = d
            nearest_zombie = zombie_rect

    return nearest_zombie

# Function to calculate the distance between two points (player and zombie)
def distance(point1, point2):
    x1, y1 = point1
    x2, y2 = point2
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5

# Function to find the nearest zombie
def find_nearest_zombie(player_rect, zombies_list):
    nearest_zombie = None
    min_distance = float('inf')
    
    for zombie_rect in zombies_list:
        d = distance(player_rect.center, zombie_rect.center)
        if d < min_distance:
            min_distance = d
            nearest_zombie = zombie_rect
    
    return nearest_zombie

def rotate_player_towards_nearest_zombie(player_rect, nearest_zombie):
    dx = nearest_zombie.centerx - player_rect.centerx
    dy = nearest_zombie.centery - player_rect.centery
    angle = math.degrees(math.atan2(dy, dx))
    
    rotated_player = pygame.transform.rotate(man_image1, -angle)
    new_player_rect = rotated_player.get_rect(center=player_rect.center)
    
    return rotated_player, new_player_rect

def move_zombies_towards_player(player_rect, zombies_list, zombie_speed):
    for zombie_rect in zombies_list:
        dx = player_rect.centerx - zombie_rect.centerx
        dy = player_rect.centery - zombie_rect.centery

        distance_to_player = max(math.sqrt(dx ** 2 + dy ** 2), 1)  # Avoid division by zero

        dx /= distance_to_player
        dy /= distance_to_player

        zombie_rect.x += dx * zombie_speed
        zombie_rect.y += dy * zombie_speed

# Call the function with player_rect and zombies_list
move_zombies_towards_player(player_rect, zombies_list, zombie_speed)

def start_game():
    import survival_game_main_workings
    survival_game_main()
    # survival_game_main()
    
    # Add code to initialize and start your game
    pass

def show_about():
    # Add code to display information about the game
    pass

def open_settings():
    # Add code to open game settings
    pass


play_image = pygame.image.load("assets/play.png")
pause_image = pygame.image.load("assets/pause.png")
current_image = play_image


window_name_set = "ZOMBIE SURVIVAL GAME"

def main_frame_setup():
    global zombie_image
    pygame.init()
    pygame.display.set_caption("ZOMBIE SURVIVAL GAME")
    FPS = 40
    clock = pygame.time.Clock()
    run = True

    while run:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                run = False
            # if event.type == pygame.MOUSEBUTTONDOWN:
            #     if event.button == 1:
            #         pass
            #         # key_pressed = pygame.key.get_pressed()
                    # handle_mouse_click(key_pressed)

                    # handle_mouse_click(event.pos,key_pressed)
            key_pressed = pygame.key.get_pressed()
            handle_mouse_click(key_pressed)
        win.fill((0, 0, 0))  # Clear the screen
        win.blit(man_image1, player_rect)
        for zombie_rect in zombies_list:
            win.blit(zombie_image, zombie_rect)

        move_zombies_towards_player(player_rect, zombies_list, zombie_speed)  # Move zombies here

        logo_window()
        pygame.display.update()
        clock.tick(FPS)

main_frame_setup()