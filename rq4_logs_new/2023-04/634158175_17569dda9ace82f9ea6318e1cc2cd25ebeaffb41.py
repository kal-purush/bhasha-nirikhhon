#Создай собственный Шутер!
from pygame import *
from random import randint
game = True
finish = False
lost = 0
kill = 0
live = 3

class GameSprite(sprite.Sprite):
    def __init__(self, player_image, width, height, player_x, player_y, player_speed):
        super().__init__()
        self.image = transform.scale(image.load(player_image), (width,height))
        self.speed = player_speed
        self.rect = self.image.get_rect()
        self.width = width
        self.height = height
        self.rect.x = player_x
        self.rect.y = player_y
        self.direction = 'left'
    def move_hero(self):
        if keypressed[K_d] and self.rect.x <= 1000:
            self.rect.x += self.speed
        if keypressed[K_a] and self.rect.x >= 0:
            self.rect.x -= self.speed

    def reset(self):
        window.blit(self.image, (self.rect.x, self.rect.y))

    def update(self):
        self.rect.y += self.speed
        global lost
        if self.rect.y >= 500:
            self.rect.x = randint(20, 610)
            self.rect.y = 0
            lost = lost+1

        sprites_list = sprite.spritecollide(game_sprite,HC,True)
        for i in sprites_list:
            global live
            live = live-1

        sprites_list = sprite.spritecollide(game_sprite,monsters,True)
        for i in sprites_list:
            live = live-1

        sprites_list = sprite.groupcollide(HC,bullets,True, True)
        for i in sprites_list:
            HC1 = GameSprite('HC.png', 70,70, randint(20,800), 0, randint(8,16))
            HC.add(HC1)
            global kill
            kill = kill+2

        sprites_list = sprite.groupcollide(monsters,bullets,True, True)
        for i in sprites_list:
            Vort1 = GameSprite('Vortigaunt.png', 100,150, randint(20,800), 0, randint(1,4))
            monsters.add(Vort1)
            kill = kill+1 

    def fire(self):
        bullet = Bullet('Crowbar.png', 40, 80, self.rect.centerx,self.rect.top,5)
        bullets.add(bullet)

class Bullet(GameSprite):
    def update(self):
        self.rect.y -= self.speed
        if self.rect.y < 0:
            self.kill()
    



x_hero = 400
y_hero = 400
speed_hero = 10
 

FPS = 60

clock = time.Clock()
window = display.set_mode((1000,625))
display.set_caption('half-life na minimalkah')
background = transform.scale(image.load('Xen_(Half-Life).jpg'),(1000,625))
mixer.init()
mixer.music.load('black_mesa_xen_08.-Mind-Games.ogg')
mixer.music.play()
font.init()
font1 = font.SysFont('Arial', 36)
miss_enemy = font1.render('Miss' + str(lost), 1 , (255,255,255))
kill_enemy = font1.render('Kill' + str(kill), 1 , (255,255,255))
live_enemy = font1.render('live' + str(live), 1 , (255,255,255))

game_sprite = GameSprite('44146.png',200, 200, x_hero, y_hero, speed_hero)
Vort1 = GameSprite('Vortigaunt.png', 100,150, randint(20,800), 0, randint(1,4))
Vort2 = GameSprite('Vortigaunt.png', 100,150, randint(20,800), 0, randint(1,4))
Vort3 = GameSprite('Vortigaunt.png', 100,150, randint(20,800), 0, randint(1,4))
Vort4 = GameSprite('Vortigaunt.png', 100,150, randint(20,800), 0, randint(1,4))
Vort5 = GameSprite('Vortigaunt.png', 100,150, randint(20,800), 0, randint(1,4))
HC1 = GameSprite('HC.png', 70,70, randint(20,800), 0, randint(4,8))
HC2 = GameSprite('HC.png', 70,70, randint(20,800), 0, randint(4,8))
HC3 = GameSprite('HC.png', 70,70, randint(20,800), 0, randint(4,8))
bullets = sprite.Group()
HC = sprite.Group()
monsters = sprite.Group()
HC.add(HC1, HC2, HC3)
monsters.add(Vort1, Vort2, Vort3, Vort4, Vort5)
game = True
while game:
    for el in event.get():
        if el.type == QUIT:
            game = False
        elif el.type == KEYDOWN:
            if el.key == K_SPACE:
                game_sprite.fire()
    if finish == False:
        window.blit(background,(0,0))
        game_sprite.reset()
    
        #ufo.reset()
        HC.draw(window)
        HC.update()
        monsters.draw(window)
        monsters.update()
        bullets.draw(window)
        bullets.update()
        miss_enemy = font1.render('Miss: ' + str(lost), 1, (255, 255, 255))
        kill_enemy = font1.render('Kill:' + str(kill), 1 , (255,0,0))
        live_enemy = font1.render('live:' + str(live), 1 , (0,255,0))
        window.blit(miss_enemy, (10,10))
        window.blit(kill_enemy, (10,40))
        window.blit(live_enemy, (10,70))
        keypressed = key.get_pressed()
        game_sprite.move_hero()
        if lost >= 10:
            finish = True
        if kill >= 10:
            finish = True
        if live <= 0:
            finish = True

    clock.tick(FPS)
    time.delay(5)

    display.update()
    clock.tick(FPS)