import pygame
class CarObject(pygame.sprite.Sprite):
    rect = None
    sprite = None
    gravity = None
    x = None
    y = None
    dy = None
    jump_height = None
    jumping = None

    def __init__(self, sprite):
#        print("Initiate car")
        pygame.sprite.Sprite.__init__(self)
        self.jump_speed = 0
        self.gravity = 0.5 
        self.sprite = pygame.image.load('sprites/car.png').convert_alpha()
        self.rect = self.sprite.get_rect()
        self.rect.inflate(-10,-10) # Shrinks the collision rectangle to make the game a little easier.
        self.jump_velocity = 0
        self.jump_height = 0
        self.jumping = False
        self.x = 40
        self.y = 300
        self.dy = 0

    def jump(self):
        if not self.jumping:
            self.dy = -12
            self.jumping = True
        
    def update(self):
        # Jump physics formula courtesy of http://goo.gl/ETTw15
        self.rect[0] = self.x
        self.rect[1] = self.y
        
        if self.jumping:
            self.dy += self.gravity
            self.y += self.dy

            if self.y > 300:
                self.y = 300
                self.dy = 0
                self.jumping = False

    def draw(self, screen):
        screen.blit(self.sprite, (self.x, self.y))