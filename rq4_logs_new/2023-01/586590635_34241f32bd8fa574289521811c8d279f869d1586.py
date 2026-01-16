import pygame
import math
import random


class Dot:
    def __init__(self, pos, dir, vel):
        self.pos = pos
        self.dir = dir
        self.vel = vel
        self.list = []
        self.current = 0
        self.alive = True
        self.reached = False
        self.fitness = 0
        self.best = False
        self.acc = 0
        self.maxSpeed = 20
        for k in range(0, 1000):
            self.list.append([random.random() * 2 * math.pi, random.random()])

    def move(self):
        self.dir = self.list[self.current][0]
        self.acc = self.list[self.current][1]
        self.vel += self.acc
        if self.vel > self.maxSpeed:
            self.vel = self.maxSpeed
        self.current += 1
        self.pos[0] += self.vel * math.cos(self.dir)
        self.pos[1] += self.vel * math.sin(self.dir)
        self.dead()

    def dead(self):
        self.alive = False
        if self.pos[0] <= 0:
            self.pos[0] = 0
        elif self.pos[0] >= width:
            self.pos[0] = width
        elif self.pos[1] <= 0:
            self.pos[1] = 0
        elif self.pos[1] >= height:
            self.pos[1] = height
        elif math.fabs(self.pos[0] - goal[0]) < 10 and math.fabs(self.pos[1] - goal[1]) < 10:
            self.reached = True
        elif self.current == len(self.list) - 1:
            return
        else:
            self.alive = True


class Obstacle:
    def __init__(self, x, y, w, h):
        self.rect = [x, y, w, h]

    def check(self, dot):
        if self.rect[0] < dot.pos[0] < self.rect[0] + self.rect[2] and self.rect[1] < dot.pos[1] < self.rect[1] + \
                self.rect[3]:
            return True
        return False

    def draw(self):
        pygame.draw.rect(win, (0, 0, 255), self.rect)


def findFitness():
    global mutateChance
    for k in dots:
        dist = math.sqrt((goal[0] - k.pos[0]) ** 2 + (goal[1] - k.pos[1]) ** 2)
        if k.reached:
            mutateChance = 0.01
            k.fitness = 1.0 / 8.0 + 10000 / float(k.current * k.current)
        else:
            k.fitness = 1 / (dist * dist)


def naturalSelection():
    sum = 0

    nDots = []
    for k in dots:
        sum += k.fitness
    print("SumOfFitness:", sum)
    for k in range(0, len(dots)):
        r = random.random() * sum
        nDot = createChild(selectParent(r))
        nDots.append(nDot)
    return nDots


def selectParent(rand):
    rsum = 0
    for i in dots:
        rsum += i.fitness
        if rsum >= rand:
            return i

    return


def createChild(parent):
    child = Dot([500, 800], 0, 3)
    for l in range(0, len(child.list)):
        if (1 - mutateChance) > random.random():
            child.list[l] = parent.list[l]
    return child


def findBest():
    max = 0
    best = 0
    for i in dots:
        if i.fitness > max:
            max = i.fitness
            best = i
    print("Fitness:", best.fitness)
    print("Steps:", best.current)
    print("Generation:", generation)
    new = Dot([500, 800], 0, 3)
    for k in range(0, len(best.list)):
        new.list[k] = best.list[k]
    return new


pygame.init()
width = 1000
height = width
run = True

win = pygame.display.set_mode((width, height))
pygame.display.set_caption("")
dots = []
goal = (500, 20)
generation = 0
mutateChance = 0.05
for x in range(0, 300):
    dots.append(Dot([500, 800], 0, 3))
oList = []
oList.append(Obstacle(000, 200, 600, 30))
oList.append(Obstacle(400, 600, 600, 30))


# for x in range(0, 1)
# ship = pygame.image.load(r'images\ship.png')
# ship = pygame.transform.scale(ship, (50, 50))
# red = pygame.image.load(r'images\r_bullet.png')


clock = pygame.time.Clock()

while run:
    clock.tick(120)
    keys = pygame.key.get_pressed()
    for event in pygame.event.get():

        if event.type == pygame.QUIT:
            run = False
    win.fill((0, 0, 0))
    pygame.draw.circle(win, (255, 0, 0), goal, 10)
    for o in oList:
        o.draw()
    done = True
    for d in dots:
        if d.alive:
            done = False
            d.move()
            for o in oList:
                if o.check(d):
                    d.alive = False
        if d.best:

            pygame.draw.circle(win, (0, 255, 0), d.pos, 5)
        else:
            pygame.draw.circle(win, (255, 255, 255), d.pos, 5)

    #   if done:
    #       findFitness()
    #       best = findBest()
    #       best.best = True
    #       for d in dots:
    #          if d.alive:
    #               done = False
    #               d.move()
    #           if d.best:
    #               pygame.draw.circle(win, (0, 255, 0), d.pos, 5)
    #           elif d.reached:
    #               pygame.draw.circle(win, (0, 0, 255), d.pos, 5)
    #           else:
    #              pygame.draw.circle(win, (255, 255, 255), d.pos, 5)

    if done:
        findFitness()
        best = findBest()
        best.best = True

        dots = naturalSelection()
        dots.append(best)
    pygame.display.update()

pygame.quit()