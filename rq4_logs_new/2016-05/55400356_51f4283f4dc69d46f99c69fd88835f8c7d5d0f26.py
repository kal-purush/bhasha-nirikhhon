import pygame
import math
import random



class Vec2:
    def __init__(self, x = 0, y = 0):
        self.x, self.y = x, y

    def __add__(self, v):
        return Vec2(self.x + v.x, self.y + v.y)

    def __sub__(self, v):
        return Vec2(self.x - v.x, self.y - v.y)

    def __mul__(self, alpha):
        return Vec2(self.x * alpha, self.y * alpha)

    def __rmul__(self, alpha):
        return Vec2(self.x * alpha, self.y * alpha)

    def intpair(self):
        return (int(self.x), int(self.y))

    def len(self):
        return math.sqrt(self.x * self.x + self.y * self.y)


class Ball:
    def __init__(self, pos = Vec2(0,0), speed = Vec2(0,0), g = 100, rad =1 , k = 0.0 , i=1, color = (255,255,255)):
        self.pos, self.speed, self.g, self.rad, self.k, self.i, self.color = \
                pos, speed, g, rad, k, i, color

    def update(self, game):
        """Update Player state"""
        self.pos.x += self.speed.x * game.delta
        self.pos.y += self.speed.y * game.delta-0.5*self.g * game.delta * game.delta
        self.speed.y += self.g * game.delta
        self.i += 1

        """Do not let Ball get out of the Game window"""
        if self.pos.x < self.rad:
            if self.speed.x < 0:
                self.speed.x = -self.speed.x
                self.pos.x = self.rad
        if self.pos.y < self.rad:
            if self.speed.y < 0:
                self.speed.y = -self.speed.y
                self.pos.y = -self.rad
        if self.pos.x > 600-self.rad:
            if self.speed.x > 0:
                self.speed.x = -self.speed.x
                self.pos.x = 600-self.rad

        if self.pos.y > 600-self.rad:
            if self.speed.y > 0:
                self.speed.y = -self.k*self.speed.y
                self.pos.y = 600-self.rad

    def render(self, game):
        """Draw Ball on the Game window"""
        pygame.draw.circle(game.screen,self.color,
                (int(self.pos.x), int(self.pos.y)), int(self.rad))




class Block:
    color = (93, 124, 79)
    def __init__(self, x1=0, y1=0, x2=0, y2=0):
        self.x1, self.y1, self.x2, self.y2 = \
            x1, y1, x2, y2

    def render(self, game):
        pygame.draw.rect(game.screen, self.color, (self.x1, self.y1, self.x2-self.x1, self.y2-self.y1,))



def collideWithBlock(block, ball):
    if (ball.pos.x>block.x1)and(ball.pos.x<block.x2)and(ball.pos.y>block.y1-ball.rad)and(ball.pos.y<block.y2+ball.rad):
        ball.speed.x = 10*random.random()*(-1)**ball.i
        ball.speed.y = -random.random()*0




class Game:
    def tick(self):
        """Return time in seconds since previous call
        and limit speed of the game to 50 fps"""
        self.delta = self.clock.tick(50) / 1000.0

    def __init__(self):
        """Constructor of the Game"""
        self._running = True
        self.size = self.width, self.height = 600, 600
        # create main display - 640x400 window
        # try to use hardware acceleration
        self.screen = pygame.display.set_mode(self.size, pygame.HWSURFACE)
        # set window caption
        pygame.display.set_caption('Game')
        # get object to help track time
        self.clock  = pygame.time.Clock()

        # set default tool
        self.tool = 'run'
        self.ball = Ball()
        self.balls = []
        for j in range(5000):
            self.balls.append(Ball(Vec2(600*random.random(),random.random()*100),Vec2(0,0),  rad=random.randint(5, 15), color = (random.random()*255, random.random()*255, random.random()*255)))



        self.block = Block()
        self.blocks = []
        self.blocks.append(Block(500,100,600,200))
        self.blocks.append(Block(200,400,450,450))
        self.blocks.append(Block(50,200,300,250))
        self.blocks.append(Block(0,500,300,540))
        self.blocks.append(Block(400,500,550,550))
        #self.blocks.append(Block(30,23,90,21))
        #self.blocks.append(Block(50,10,20,200))
        #self.blocks.append(Block(120,90,67,30))
        #self.blocks.append(Block(40,37,78,41))
        self.units = self.balls + self.blocks
        #self.units = self.balls
        #self.units = self.blocks
        #for b in self.balls:
        #    self.units.append(b)
        #for c in self.blocks:
        #    self.units.append(c)



    def event_handler(self, event):
        """Handling one pygame event"""
        if event.type == pygame.QUIT:
            # close window event
            self.exit()
        elif event.type == pygame.KEYDOWN:
            # keyboard event on press ESC
            if event.key == pygame.K_ESCAPE:
                self.exit()

    def update(self):
        """Here game objects update their positions"""
        self.tick()

        self.ball.update(self)
        for c in self.balls:
            c.update(self)
        for c in self.balls:
            for z in self.blocks:
                collideWithBlock(z,c)


    def render(self):
        """Render the scene"""
        self.screen.fill((0, 0, 0))
        '''self.ball.render(self)
        self.block.render(self)'''



        '''for b in self.blocks:
            b.render(self)
        for c in self.balls:
            c.render(self)'''

        for n in self.units:
            n.render(self)






        pygame.display.flip()

    def exit(self):
        """Exit the game"""
        self._running = False

    def cleanup(self):
        """Cleanup the Game"""
        pygame.quit()

    def execute(self):
        """Execution loop of the game"""
        while(self._running):
            # get all pygame events from queue
            for event in pygame.event.get():
                self.event_handler(event)
            self.update()
            self.render()
        self.cleanup()





if __name__ == "__main__":
    game = Game()
    game.execute()