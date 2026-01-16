import pygame


pygame.init()

WINDOW_WIDTH = 700
WINDOW_HEIGHT = 500

display_surface = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))

pygame.display.set_caption("シンプルなピアノ")

WHITE = (255, 255, 255)
BLACK = (0, 0, 0)

display_surface.fill(BLACK)

FPS = 60
clock = pygame.time.Clock()

def text_board(font, size, text, color, x, y):
    font = pygame.font.Font(font, size)
    textsurf = font.render(text, True, color)
    textsurf_rect = textsurf.get_rect()
    textsurf_rect.center = (x, y)
    display_surface.blit(textsurf, textsurf_rect)

text_board('NotoSansJP-Regular.ttf', 50, 'シンプルなピアノ', WHITE, WINDOW_WIDTH // 2, 100)

pygame.draw.rect(display_surface, WHITE, (0, WINDOW_HEIGHT // 2, WINDOW_WIDTH, WINDOW_HEIGHT // 2))

for c in range(8):
    pygame.draw.rect(display_surface, BLACK, (6 + (WINDOW_WIDTH // 8 * c), WINDOW_HEIGHT // 2, WINDOW_WIDTH // 8 - 10, WINDOW_HEIGHT // 2 - 10), 2)


running = True

while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    pygame.display.update()
    
    clock.tick(FPS)

pygame.quit()