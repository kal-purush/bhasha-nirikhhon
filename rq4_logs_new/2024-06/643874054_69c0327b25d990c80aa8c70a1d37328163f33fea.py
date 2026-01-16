import pygame


pygame.init()

WINDOW_WIDTH = 600
WINDOW_HEIGHT = 600

display_surface = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))

pygame.display.set_caption("足し算ゲーム")

WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
RED = (255, 0, 0)
YELLOW = (255, 255, 0)
LemonChiffon = (255, 250, 205)
STEELBLUE = (70, 130, 180)
GOLD = (225, 223, 0)

display_surface.fill(BLACK)

FPS = 60
clock = pygame.time.Clock()

def text_board(font, size, text, color, x, y):
    font = pygame.font.Font(font, size)
    textsurf = font.render(text, True, color)
    textsurf_rect = textsurf.get_rect()
    textsurf_rect.center = (x, y)
    display_surface.blit(textsurf, textsurf_rect)

text_board('NotoSansJP-Regular.ttf', 50, '足し算ゲーム', WHITE, WINDOW_WIDTH // 2, 50)

class Game():
    def __init__(self):
        self.lineCreate()
    
    def lineCreate(self):
        pygame.draw.rect(display_surface, WHITE, (50, 120, 500, 150), 3)
        pygame.draw.rect(display_surface, WHITE, (25, 305, 100, 150), 3)
        pygame.draw.rect(display_surface, WHITE, (175, 305, 100, 150), 3)
        pygame.draw.rect(display_surface, WHITE, (325, 305, 100, 150), 3)
        pygame.draw.rect(display_surface, WHITE, (475, 305, 100, 150), 3)

game = Game()


running = True

while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
    
    pygame.display.update()
    
    clock.tick(FPS)

pygame.quit()