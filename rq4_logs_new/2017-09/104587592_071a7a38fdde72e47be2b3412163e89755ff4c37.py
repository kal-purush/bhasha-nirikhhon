import os
import sys
import pygame as pg

CAPTION = "YSU Hackron Project"
SCREEN_SIZE = (1400, 1050)
BACKGROUND_COLOR = (118, 118, 118)
TRANSPARENT = (0, 0, 0, 0)
COLOR_KEY = (255, 0, 255)

class Control(object):
    """Window information and main loop"""
    def __init__(self):
        """Initialize standard attributes standardly"""
        self.screen = pg.display.get_surface()
        self.done = False
        self.keys = pg.key.get_pressed()


    def event_loop(self):
        for event in pg.event.get():
            if event.type == pg.QUIT or self.keys[pg.K_ESCAPE]:
                self.done = True

    def draw(self):
        """Draw all elements to display"""
        self.screen.fill(BACKGROUND_COLOR)

    def main_loop(self):
        while not self.done:
            self.event_loop()
            self.draw()
            pg.display.update()


def main():
    os.environ['SDL_VIDEO_CENTERED'] = '1'
    pg.init()
    pg.display.set_caption(CAPTION)
    pg.display.set_mode(SCREEN_SIZE)
    Control().main_loop()
    pg.quit()
    sys.exit()


if __name__ == "__main__":
    main()