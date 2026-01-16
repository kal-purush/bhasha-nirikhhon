import pygame
from pygame.locals import *
import pygame.color

from pythontuio import TuioClient
from pythontuio import Cursor
from pythontuio import TuioListener
from threading import Thread


class MyListener(TuioListener):
    def add_tuio_cursor(self, cursor: Cursor):
        print("Added {}".format(cursor.session_id))

client = TuioClient(("localhost",3333))
t = Thread(target=client.start)
listener = MyListener()
client.add_listener(listener)

t.start()

pygame.init()
WINDOW_SIZE = (800,600)
screen = pygame.display.set_mode(WINDOW_SIZE)
pygame.display.set_caption("sam-k0's TUIO Client")

def draw_number(number, x, y):
    font = pygame.font.Font(None, 36)  # Choose the desired font and size
    text_surface = font.render(str(number), True, (255, 255, 255))  # Render the text
    text_rect = text_surface.get_rect()
    text_rect.center = (x, y)  # Set the position of the text
    screen.blit(text_surface, text_rect)  # Draw the text onto the screen

def draw_cursors(cursors:list()):
    screen.fill((0,0,0,255))
    curs:Cursor
    for curs in cursors:
        x,y = curs.position[0]*WINDOW_SIZE[0], curs.position[1]*WINDOW_SIZE[1]
        pygame.draw.circle(screen, (255,0,255,255), (int(x),int(y)), 10)
        draw_number(curs.session_id, x,y)



def main():

    dorun = True
    while dorun:
        for event in pygame.event.get():
            if event.type ==QUIT:
                dorun = False
        mycurs = client.cursors
        draw_cursors(mycurs)

        pygame.display.flip()
    pygame.quit()

if __name__ == '__main__':
    main()