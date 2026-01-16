import pygame
import os
from Board.Board import *


class Menu:
    def __init__(self, width, height):
        self.width = width
        self.height = height
        self.rendering = True
        self.screen = pygame.display.set_mode((self.width, self.height))
        self.fps = 60

    def draw(self):
        data = pygame.font.Font(None, self.height // 12).render("Heroes of the frozen throne", True,
                                                                pygame.Color("White"))
        self.screen.blit(data, ((self.width - data.get_width()) // 2,
                         self.height // 4 - data.get_height() // 2,
                         data.get_width(), data.get_height()))
        texts_for_buttons = ["Start game", "About game", "Exit"]
        for i in range(3):
            pygame.draw.rect(self.screen, pygame.Color("White"),
                             (self.width // 3, self.height // 2 + (self.height // 12 + self.height // 24) * i,
                             self.width // 3, self.height // 12), 3)
            data = pygame.font.Font(None, self.height // 24).render(texts_for_buttons[i], True,
                                                                    pygame.Color("White"))
            self.screen.blit(data, ((self.width - data.get_width()) // 2,
                                    self.height // 2 + (self.height // 12
                                                        + self.height // 24) * i
                                    + (self.height // 12 - data.get_height()) // 2,
                                    data.get_width(), data.get_height()))

    def close(self):
        self.rendering = False

    def start_game(self):
        game = Board(1080, 720, 20)
        game.render()
        self.screen = pygame.display.set_mode((self.width, self.height))

    def about_game(self):
        try:
            os.startfile("..\Data\info.txt")
        except FileNotFoundError:
            pass

    def click(self, pos):
        functions = [self.start_game, self.about_game, self.close]
        x, y = pos[0], pos[1]
        for i in range(len(functions)):
            start_pos = self.width // 3, self.height // 2 + (self.height // 12 + self.height // 24) * i
            w, h = self.width, self.height // 12
            if start_pos[0] <= x <= start_pos[0] + w and start_pos[1] <= y <= start_pos[1] + h:
                functions[i]()

    def render(self):
        pygame.init()
        clock = pygame.time.Clock()
        while self.rendering:
            self.screen.fill((0, 0, 0))
            self.draw()
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    self.rendering = False
                elif event.type == pygame.MOUSEBUTTONDOWN:
                    self.click(event.pos)
            pygame.display.flip()
            clock.tick(self.fps)


test = Menu(480, 480)
test.render()