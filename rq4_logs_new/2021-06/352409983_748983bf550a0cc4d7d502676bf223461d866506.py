from pygame import Rect
from pygame.draw import ellipse as draw_ellipse


class Ball():

    def __init__(self, screen, color, pos_x, pos_y, size, move_x, move_y):
        self.screen = screen
        self.color = color
        self.rect = Rect(pos_x, pos_y, size, size)
        self.move_x = move_x
        self.move_y = move_y
        self.ball = draw_ellipse(self.screen, self.color, self.rect)

    def draw_ball(self):
        self.ball = draw_ellipse(self.screen, self.color, self.rect)

    def move_ball(self):

        if self.rect.top > (self.screen.get_height() - self.rect.height) or self.rect.top < 0:
            self.move_y *= -1

        if self.rect.left > (self.screen.get_width() - self.rect.width) or self.rect.left < 0:
            self.move_x *= -1

        self.rect.move_ip(0, self.move_y)
        self.rect.move_ip(self.move_x, 0)
  
    def racket_shoot(self):
        self.move_x *= -1
        self.rect.move_ip(self.move_x * 2, 0)