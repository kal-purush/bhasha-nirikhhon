from re import S
import pygame
import Settings
from menu import *

class Game():
    def __init__(self):
        pygame.init()
        self.running = True
        self.playing = False
        self.UP_KEY, self.DOWN_KEY, self.START_KEY, self.BACK_KEY = False, False, False, False
        self.display = pygame.Surface((Settings.WINDOW_WIDTH, Settings.WINDOW_HEIGHT))
        self.window = pygame.display.set_mode(((Settings.WINDOW_WIDTH,Settings.WINDOW_HEIGHT)))
        self.font_name = pygame.font.get_default_font()
        self.WHITE, self.BLACK, self.RED = (255, 255, 255), (0, 0, 0), (255, 0, 0)
        self.main_menu = MainMenu(self)
        self.options = ControlsMenu(self)
        self.curr_menu = self.main_menu

    def game_loop(self):
        while self.playing:
            self.check_events()
            if self.START_KEY:
                self.playing = False
            self.display.fill(self.BLACK)
            self.draw_text('Level 1', 20, Settings.WINDOW_WIDTH/2, Settings.WINDOW_HEIGHT/2)
            #this is where you would add our game levels
            self.window.blit(self.display, (0,0))
            pygame.display.update()
            self.reset_keys()

    def check_events(self):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                self.running, self.playing = False, False
                self.curr_menu.run_display = False
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_RETURN:
                    self.START_KEY = True
                if event.key == pygame.K_BACKSPACE:
                    self.BACK_KEY = True
                if event.key == pygame.K_DOWN:
                    self.DOWN_KEY = True
                if event.key == pygame.K_UP:
                    self.UP_KEY = True

    def reset_keys(self):
        self.UP_KEY, self.DOWN_KEY, self.START_KEY, self.BACK_KEY = False, False, False, False

    def draw_text(self, text, size, x, y):
        font = pygame.font.Font(self.font_name, size)
        text_surface = font.render(text, True, self.RED)
        text_rect = text_surface.get_rect()
        text_rect.center = (x,y)
        self.display.blit(text_surface, text_rect)
