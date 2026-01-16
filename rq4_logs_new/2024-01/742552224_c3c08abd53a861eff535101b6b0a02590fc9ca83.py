from pygame import *
from random import randint

class GameSpirte(sprite.Sprite):
    def __init__(self, player_image, player_x, player_y, player_speed, w = 100, h = 100):
        super().__init__()
        self.image = transform.scale(image.load(player_image), (w, h))
        self.speed = player_speed
        self.rect = self.image.get_rect()
        self.rect.x = player_x
        self.rect.y = player_y

    def reset(self):
        window.blit(self.image, (self.rect.x, self.rect.y))

class Bulett(GameSpirte):
    def __init__(self, player_image, player_x, player_y, player_speed, w=100, h=100, btype=0):
        super().__init__(player_image, player_x, player_y, player_speed, w, h)
        self.btype = btype
    def update(self):
        self.rect.y += self.speed
        if self.btype == -1:
            self.rect.x -= self.speed / 2
        if self.btype == 1:
            self.rect.x += self.speed / 2
        if self.rect.y < 0:
            self.kill()

class Player(GameSpirte):
    def update(self):
        keys = key.get_pressed()
        if keys[K_LEFT] and self.rect.x >5:
            self.rect.x -= self.speed
        if keys[K_RIGHT] and self.rect.x < win_width - 80:
            self.rect.x += self.speed
    def fire(self):
        bullet = Bulett("загружено_пуля-removebg-preview.png~", self.rect.centerx, self.rect.top, -15, 15, 20)
        bullets.add(bullet)
        if is_upgraded:
            bullet2 = Bulett("загружено_пуля-removebg-preview.png~", self.rect.centerx, self.rect.top, -15, 20, 20, -1)
            bullet3 = Bulett("загружено_пуля-removebg-preview.png~", self.rect.centerx, self.rect.top, -15, 20, 20, 1)
            bullets.add(bullet2, bullet3)

class Enemy(GameSpirte):
    def update(self):
        self.rect.y += self.speed
        global lost
        if self.rect.y > win_height:
            self.rect.x = randint(80, win_width - 80)
            self.rect.y = 0
            lost = lost + 1

class Anemy(GameSpirte):
    def update(self):
        self.rect.y += self.speed
        if self.rect.y > win_height:
            self.rect.x = randint(80, win_width - 80)
            self.rect.y = 0

font.init()
font1 = font.Font(None, 36)
font2 = font.Font(None, 80)
win = font2.render("YOU WIN!", True, (255, 255, 255))
lose = font2.render("YOU LOSE!", True, (180, 0, 0))

win_width = 800; win_height = 800
window = display.set_mode((win_width, win_height))

bullets = sprite.Group()

up_level = Anemy('images-removebg-preview.png', randint(80, win_width - 80), -40, randint(1, 2))
is_upgraded = False
is_on_screen = True

up_level = Anemy("images-removebg-preview.png", randint(80, win_width - 80), -40, 2, 50, 50)

player = Player("images-removebg-preview сніговик.png~~", 5, win_height - 100, 5)

monsters = sprite.Group()
for i in range(1,9):
    monster = Enemy("Untitled.png", randint(80, win_width - 80), -40, randint(1, 2), 50, 50)
    monsters.add(monster)

display.set_caption("ШУТЕР!")
background = transform.scale(image.load("landscape-4766943_1280.jpg"), (win_width, win_height))
clock = time.Clock()

goal = 201
max_lost = 5
lost = 0
FPS = 60
score = 0
life = 5

game = True
finish = False
while game:

    for e in event.get():
        if e.type == QUIT:
            game = False
        elif e.type == KEYDOWN:
            if e.key == K_SPACE:
                player.fire()
            elif e.key == K_r and finish == True:
                finish = False 
                lost = 0
                score = 0
                for b in bullets:
                    b.kill()
                for s in monsters:
                    s.kill()
                for i in range(1,9):
                    monster = Enemy("Untitled.png", randint(80, win_width - 80), -40, randint(1, 2), 50, 50)
                    monsters.add(monster)
                 

    if not finish:
        window.blit(background, (0, 0)
        )
        text_lose = font1.render("Пропущено: " + str(lost), 1, (255, 255, 255))
        window.blit(text_lose, (10, 50))

        Text = font1.render("Рахунок: " + str(score), 1, (255, 255, 255))
        window.blit(Text, (10, 20))

        player.reset()
        player.update()

        bullets.draw(window)
        bullets.update()

        monsters.update()
        monsters.draw(window)

        if score > 75 and is_on_screen:
            up_level.reset()
            up_level.update()

        if sprite.collide_rect(player, up_level):
            is_upgraded = True
            is_on_screen = False

        if sprite.groupcollide(monsters, bullets, True, True):
            score = score + 1
            monster = Enemy("Untitled.png", randint(80, win_width - 80), -40, randint(1, 2), 50, 50)
            monsters.add(monster)

        if score >= goal:
            finish = True
            window.blit(win, (200, 200))

        if sprite.spritecollide(player, monsters, True):
            life = life - 1

        if life == 0 or lost >= max_lost:
            finish = True
            window.blit(lose, (300, 350))

        if life == 5 or life == 4:
            life_color = (2, 115, 0)

        if life == 3:
            life_color = (230, 205, 0)

        if life == 2 or life == 1:
            life_color = (197, 0, 0)
        
        text_life = font1.render("Життів: " + str(life), 1, life_color)
        window.blit(text_life, (10, 80))

    else:
        finish = False
        score = 0
        lost = 0
        life = 5
        for b in bullets:
            b.kill()
        for s in monsters:
            s.kill()
        time.delay(3000)
        for i in range(1,9):
            monster = Enemy("Untitled.png", randint(80, win_width - 80), -40, randint(1, 3), 50, 50)
            monsters.add(monster)

    display.update()
    clock.tick(FPS)