import socket
import time
import pygame
import os.path
import random
from PIL import Image

serv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
serv_sock.bind(('localhost', 1488))
serv_sock.setblocking(0)
serv_sock.listen(10)


#Игра

Ball_sprites = pygame.sprite.Group()

def new_ball(r, x, y, color):
    i = Ball(20, x - 20, y - 20, (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    if i.update():
        all_sprites.remove_internal(i)
        Ball_sprites.remove_internal(i)
class Ball(pygame.sprite.Sprite):
    def __init__(self, radius, x, y, color):
        super().__init__(all_sprites)

        self.x = x
        self.y = y
        self.c = color
        self.radius = radius
        self.image = pygame.Surface((2 * radius, 2 * radius),
                                    pygame.SRCALPHA, 32)
        pygame.draw.circle(self.image, pygame.Color(color),
                           (radius, radius), radius)
        self.rect = pygame.Rect(x, y, 2 * radius, 2 * radius)
        while True:
            self.vx = random.randint(-5, 5)
            self.vy = random.randrange(-5, 5)
            if self.vy or self.vx:
                break
        self.add(Ball_sprites)

    def update(self):
        self.rect = self.rect.move(self.vx, self.vy)
        self.x = self.x + self.vx
        self.y = self.y + self.vy
        if pygame.sprite.spritecollideany(self, horizontal_borders):
            self.vy = -self.vy
            return 1
        if pygame.sprite.spritecollideany(self, vertical_borders):
            self.vx = -self.vx
            return 1
        for i in Ball_sprites:
            if i.rect != self.rect:
                if pygame.sprite.collide_circle(self, i):
                    self.vy = -self.vy
                    self.vx = -self.vx
                    return 1

def new_ball(r, x, y, color):
    i = Ball(r, x, y, color)
    if i.update():
        all_sprites.remove_internal(i)
        Ball_sprites.remove_internal(i)

horizontal_borders = pygame.sprite.Group()
vertical_borders = pygame.sprite.Group()
class Border(pygame.sprite.Sprite):
    # строго вертикальный или строго горизонтальный отрезок
    def __init__(self, x1, y1, x2, y2):
        super().__init__(all_sprites)
        if x1 == x2:  # вертикальная стенка
            self.add(vertical_borders)
            self.image = pygame.Surface([1, y2 - y1])
            self.rect = pygame.Rect(x1, y1, 1, y2 - y1)
        else:  # горизонтальная стенка
            self.add(horizontal_borders)
            self.image = pygame.Surface([x2 - x1, 1])
            self.rect = pygame.Rect(x1, y1, x2 - x1, 1)


pygame.init()
size = width, height = 600, 600
screen = pygame.display.set_mode(size)

all_sprites = pygame.sprite.Group()

Border(0, 0, width, 0)
Border(0, height - 0, width, height - 0)
Border(0, 0, 0, height)
Border(width, 0, width, height)

running = 1
fps = 60
clock = pygame.time.Clock()
#игра


all_sockets =[]
while running:

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

        if event.type == pygame.MOUSEBUTTONDOWN:
            x, y = event.pos
            new_ball(20, x - 20, y - 20, (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))

    screen.fill((0, 0, 0))

    all_sprites.draw(screen)
    all_sprites.update()

    pygame.display.flip()
    clock.tick(fps)

    try:
        client_sock, client_addr = serv_sock.accept()
        print('Connected by', client_addr)
        client_sock.setblocking(0)
        all_sockets.append(client_sock)

    except:
        pass

    for sock in all_sockets:
        try:
            data = sock.recv(1024).decode()
            if data != '':
                x, y = data.split()
                new_ball(20, int(x) - 20, int(y) - 20, (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
        except:
            pass

    data = []
    for i in Ball_sprites:
        data.append(str(i.radius) + ' ' + str(i.x) + ' ' + str(i.y) + ' ' + str(i.c))

    data = '<' + (';' .join(data)) + '>'

    for sock in all_sockets:
        try:
            sock.send(data.encode())
        except:
            all_sockets.remove(sock)
            sock.close()

pygame.quit()