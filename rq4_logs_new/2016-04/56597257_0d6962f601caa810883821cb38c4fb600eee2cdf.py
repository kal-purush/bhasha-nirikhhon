#sounds from wavsource.com

import turtle
import random
import time
import winsound

#class for shots fired
class Ammo:
    def __init__(self, x, y, heading, game, min_x, max_x, min_y, max_y, mute):
        self.g = game
        self.t = turtle.Turtle()
        self.t.up()
        self.t.speed(0)
        self.t.shape('circle')
        self.t.shapesize(.4,.4,.4)
        self.t.color('red')
        self.t.goto(x,y)
        self.t.setheading(heading)
        self.min_x = min_x
        self.max_x = max_x
        self.min_y = min_y
        self.max_y = max_y
        self.done = False
        self.pause = False
        self.g.addAmmo(self)
        if not mute:
            winsound.PlaySound('sounds\peeeooop_x.wav',\
                               winsound.SND_FILENAME|winsound.SND_ASYNC)
        self.move()
    #what to do in one movement timestep
    def move(self):
        if not self.done and not self.pause:
            self.t.forward(10)
            if not ((self.min_x <= self.t.xcor() <= self.max_x) \
                and (self.min_y <= self.t.ycor() <= self.max_y)):
                self.end()
            else:
                self.g.ontimer(self.move, 25)
    #return the ammo's current position on the board
    def pos(self):
        return self.t.pos()
    #switch ammo between being paused and not paused
    def togglePause(self):
        self.pause = not self.pause
        if not self.pause:
            self.move()
    #ammo either goes out-of-bounds or hits the enemy
    def end(self):
        self.t.hideturtle()
        self.done = True
        self.g.removeAmmo(self)


#class for player-controlled character
class Character:
    def __init__(self, game, min_x, max_x, min_y, max_y):
        self.t = turtle.Turtle()
        self.t.shape('turtle')
        self.t.color('blue')
        self.t.up()
        self.moveLen = 2.5
        self.forwardMovement = False
        self.done = False
        self.canShoot = True
        self.mute = False
        self.pause = False
        #screen stuff
        self.g = game
        self.min_x = min_x
        self.max_x = max_x
        self.min_y = min_y
        self.max_y = max_y
        #bind keypress functions
        self.g.onkey(self.turnRight, "Right")
        self.g.onkey(self.turnRight, "d")
        self.g.onkey(self.turnLeft, "Left")
        self.g.onkey(self.turnLeft, "a")
        self.g.onkey(self.turnAround, "Down")
        self.g.onkey(self.turnAround, "s")
        self.g.onkeypress(self.startForward, "Up")
        self.g.onkeypress(self.startForward, "w")
        self.g.onkeyrelease(self.stopForward, "Up")
        self.g.onkeyrelease(self.stopForward, "w")
    #turn character 90 degrees right
    def turnRight(self):
        if not self.pause:
            self.t.setheading(self.t.heading() - 90)
    #turn character 90 degrees left
    def turnLeft(self):
        if not self.pause:
            self.t.setheading(self.t.heading() + 90)
    #turn character around entirely
    def turnAround(self):
        if not self.pause:
            self.t.setheading(self.t.heading() + 180)
    #start having character move forward
    def startForward(self):
        self.forwardMovement = True
    #cease having character move forward
    def stopForward(self):
        self.forwardMovement = False
    #fire a shot in current heading
    def shoot(self):
        heading = self.t.heading()
        if (not self.canShoot) or self.pause or (heading%90 != 0):
            return
        Ammo(self.t.xcor(),self.t.ycor(),heading,self.g,\
             self.min_x,self.max_x,self.min_y,self.max_y, self.mute)
        self.canShoot = False
        self.g.ontimer(self.enableShooting, 500)
    #allow character to fire another shot
    def enableShooting(self):
        self.canShoot = True
    #what to do in one movement timestep
    def move(self):
        if not self.done and not self.pause:
            if self.forwardMovement:
                self.t.forward(self.moveLen)
            #check for turtle being out of the board
            toofar = False
            x = self.t.xcor()
            y = self.t.ycor()
            if x > self.max_x:
                x = self.max_x
                toofar = True
            elif x < self.min_x:
                x = self.min_x
                toofar = True
            if y > self.max_y:
                y = self.max_y
                toofar = True
            elif y < self.min_y:
                y = self.min_y
                toofar = True
            if toofar:
                self.t.goto(x,y)
            #set up to move again in the future
            self.g.ontimer(self.move, 25)
    #start the enemy moving
    def start(self):
        self.g.onkey(self.shoot, "space")
        self.enableShooting()
        self.done = False
        self.unpause()
        self.move()
    #put character back to state at start of game
    def reset(self):
        self.t.setheading(0)
        self.t.goto(0,0)
    #return the character's current position on the board
    def pos(self):
        return self.t.pos()
    #place the character in a certain spot
    def goto(self,x,y):
        self.t.goto(x,y)
    #switch character between being muted and not muted
    def toggleMute(self):
        self.mute = not self.mute
    #switch character between being paused and not paused
    def togglePause(self):
        self.pause = not self.pause
        if not self.pause:
            self.move()
    #change character being paused to false
    def unpause(self):
        self.pause = False
    #set character game to being done
    def finish(self):
        self.done = True
        self.canShoot = False


#class for computer-controlled character
class Enemy:
    def __init__(self, game, min_x, max_x, min_y, max_y):
        #screen stuff
        self.g = game
        self.min_x = min_x
        self.max_x = max_x
        self.min_y = min_y
        self.max_y = max_y
        #turtle stuff
        self.t = turtle.Turtle()
        self.t.shape('turtle')
        self.t.color('orange')
        self.t.up()
        self.reset()
        self.minSteps = 20
        self.mute = False
        self.pause = False
        #start the loop
        self.done = False
    #what to do in one movement time step
    def move(self):
        if not self.done and not self.pause:
            goodAngles = self.g.badEnemyDirection()
            if self.stepsTaken >= self.minSteps:
                if self.t.heading() in goodAngles:
                    goodAngles.append(self.t.heading()) #bias toward forward motion
                direction = random.choice(goodAngles)
                self.t.setheading(direction)
                self.stepsTaken = 0
            elif self.t.heading() not in goodAngles: #heading toward hero
                direction = random.choice(goodAngles)
                self.t.setheading(direction)
            self.t.forward(self.moveLen)
            self.stepsTaken += 1
            if not ((self.min_x <= self.t.xcor() <= self.max_x) \
                    and (self.min_y <= self.t.ycor() <= self.max_y)):
                self.t.setheading(self.t.heading() + 180)
                self.t.forward(self.moveLen)
            self.g.ontimer(self.move, 25)
    #start the enemy moving
    def start(self):
        self.done = False
        self.unpause()
        self.move()
    #put enemy back to state at start of game
    def reset(self):
        self.t.goto(random.randint(self.min_x, self.max_x), \
                    random.randint(self.min_y, self.max_y))
        self.t.setheading(0)
        self.stepsTaken = 0
        self.moveLen = 7.5
    #return the enemy's current position on the screen
    def pos(self):
        return self.t.pos()
    #what to do when a shot hits
    def hit(self):
        self.moveLen = self.moveLen * .9
        if not self.mute:
            winsound.PlaySound('sounds\scream_male.wav', \
                               winsound.SND_FILENAME|winsound.SND_ASYNC)
        self.t.color('red')
        self.g.ontimer((lambda: self.t.color('orange')), 300)
    #slow enemy down a lot--it's a cheat
    def nuke(self):
        if not self.mute:
            winsound.PlaySound('sounds\explosion_x.wav',\
                               winsound.SND_FILENAME|winsound.SND_ASYNC)
        self.moveLen = self.moveLen * .5
        self.t.shape('circle')
        self.t.shapesize(2,2,1)
        self.t.color('yellow')
        self.g.ontimer((lambda: self.t.color('orange')), 300)
        self.g.ontimer((lambda: self.t.shape('turtle')), 300)
        self.g.ontimer((lambda: self.t.shapesize(1,1,1)), 300)
    #switch enemy between being muted and not muted
    def toggleMute(self):
        self.mute = not self.mute
    #switch enemy between being paused and not paused
    def togglePause(self):
        self.pause = not self.pause
        if not self.pause:
            self.move()
    #change enemy being paused to false
    def unpause(self):
        self.pause = False
    #set enemy to game being done
    def finish(self):
        self.done = True


#class to control whole game
class Game:
    def __init__(self):
        #make screeen for game
        self.screen = turtle.Screen()
        self.screen.setup(width=.95,height=.95,startx=0,starty=0)
        self.screen.title('Fighting Turtles')
        self.screen.bgcolor('blue')
        self.screen.delay(0)
        w = self.screen.window_width()
        h = self.screen.window_height()
        self.max_x = int(w/2) - 30
        self.min_x = int(-w/2) + 20
        self.max_y = int(h/2) - 50
        self.min_y = int(-h/2) + 30
        #display the awesome title
        self.playTitle()
        #set up attributes
        self.liveAmmo = []
        self.mute = False
        self.pause = False
        self.finished = True #kind of like a self.running thing
        self.time = 0
        self.shotsFired = 0
        self.hits = 0
        #make turtle for writing with
        self.writeTurtle = turtle.Turtle()
        self.writeTurtle.hideturtle()
        self.writeTurtle.speed(0)
        #draw game area
        self.setup()
        #make turtle for writing time with
        self.timeTurtle = turtle.Turtle()
        self.timeTurtle.hideturtle()
        self.timeTurtle.speed(0)
        self.timeTurtle.up()
        self.timeTurtle.color('white')
        self.timeTurtle.goto(0, self.max_y + 20)
        self.writeTime()
        #make turtle for writing number of shots fired with
        self.shotsFiredTurtle = turtle.Turtle()
        self.shotsFiredTurtle.hideturtle()
        self.shotsFiredTurtle.speed(0)
        self.shotsFiredTurtle.up()
        self.shotsFiredTurtle.color('white')
        self.shotsFiredTurtle.goto(self.min_x + 25, self.max_y + 20)
        self.writeShots()
        #make turtle for writing number of hits
        self.hitsTurtle = turtle.Turtle()
        self.hitsTurtle.hideturtle()
        self.hitsTurtle.speed(0)
        self.hitsTurtle.up()
        self.hitsTurtle.color('white')
        self.hitsTurtle.goto(self.max_x - 25, self.max_y + 20)
        self.writeHits()
        #create characters
        self.character = Character(self, self.min_x, self.max_x, self.min_y, self.max_y)
        self.other = Enemy(self, self.min_x, self.max_x, self.min_y, self.max_y)
        #bind keys for mute and pause
        self.screen.onkey(self.toggleMute, "m")
        self.screen.onkey(self.togglePause, "p")
        #start window listening for events
        self.screen.listen()
        self.screen.mainloop()
    #play the awesome title across the screen
    def playTitle(self):
        self.screen.register_shape('images\\fighting.gif')
        self.screen.register_shape('images\\turtles.gif')
        ft = turtle.Turtle()
        ft.up()
        ft.speed(0)
        ft.shape('images\\fighting.gif')
        tt = turtle.Turtle()
        tt.up()
        tt.speed(0)
        tt.shape('images\\turtles.gif')
        ft.goto(self.min_x,100)
        tt.goto(self.max_x,-100)
        ft.speed(1)
        tt.speed(1)
        rate = 0.9
        steps = 60
        cheatdist = 40
        originaldistance = 40
        delta = 2/steps * (originaldistance - abs(self.min_x)/(steps+1))
        distance = originaldistance
        i = 0
        while i < steps:
            ft.forward(distance)
            tt.backward(distance)
            distance -= delta
            i += 1
            time.sleep(0.01)
        i = 0
        while i < steps:
            ft.forward(distance)
            tt.backward(distance)
            distance += delta
            i += 1
            time.sleep(0.01)
        ft.hideturtle()
        tt.hideturtle()
    #draw the playing field
    def setup(self):
        #create turtle to draw with
        self.drawTurtle = turtle.Turtle()
        self.drawTurtle.hideturtle()
        self.drawTurtle.up()
        self.drawTurtle.speed(0)
        self.drawTurtle.goto(self.min_x-10, self.min_y-10)
        #draw game field
        self.drawTurtle.fillcolor('forest green')
        self.drawTurtle.begin_fill()
        self.drawTurtle.down()
        self.drawTurtle.width(10)
        self.drawTurtle.goto(self.max_x+10, self.min_y-10)
        self.drawTurtle.goto(self.max_x+10, self.max_y+10)
        self.drawTurtle.goto(self.min_x-10, self.max_y+10)
        self.drawTurtle.goto(self.min_x-10, self.min_y-10)
        self.drawTurtle.end_fill()
        #display messages and permitting starting and ending game with keys
        self.writeMessage()
        self.screen.onkey(self.start, "space")
        self.screen.onkey(self.Quit, "Escape")
        self.screen.onkey(self.cheat, "c")
        self.screen.onkey(self.nuke, "n")
    #write the message displayed at game start
    def writeMessage(self):
        self.writeTurtle.up()
        self.writeTurtle.home()
        self.writeTurtle.write('Press space to start', align='center', \
                               font=('Arial',24,'bold'))
        self.writeTurtle.goto(0, self.min_y+20)
        self.writeTurtle.write('Space to shoot, up to go forward, right to turn right, left to turn left,',\
                               align='center', font=('Arial',14,'normal'))
        self.writeTurtle.goto(0, self.min_y)
        self.writeTurtle.write('down to turn around', align='center',
                               font=('Arial',14,'normal'))
        self.writeTurtle.goto(0, self.max_y-20)
        self.writeTurtle.write('You need to grab the other turtle, but he is faster than you.',
                               align='center', font=('Arial',14,'normal'))
        self.writeTurtle.goto(0, self.max_y-40)
        self.writeTurtle.write('Shoot him to slow him down until you can catch him.',
                               align='center', font=('Arial',14,'normal'))
    #start running game
    def start(self, x=None, y=None):
        self.screen.onkey(None, "space")
        self.writeTurtle.clear()
        self.finished = False
        self.pause = False
        self.cheatsUsed = 0
        self.startTime = time.time()
        self.character.start()
        self.other.start()
        self.updateTime()
        self.timeStep()
    #prepare to start again
    def reset(self):
        self.screen.onkey(None, "r")
        self.writeTurtle.clear()
        self.screen.onkey(self.start, "space")
        self.character.reset()
        self.other.reset()
        self.time = 0
        self.writeTime()
        self.shotsFired = 0
        self.writeShots()
        self.hits = 0
        self.writeHits()
        self.writeMessage()
    #check for collisions at set intervals between hero, enemy, and shots
    def timeStep(self):
        if self.finished or self.pause:
            return
        ox,oy = self.other.pos()
        cx,cy = self.character.pos()
        if abs(ox-cx) < 10 and abs(oy-cy) < 10:
            self.end()
        else:
            for i in self.liveAmmo:
                x,y = i.pos()
                if abs(ox-x) < 15 and abs(oy-y) < 15:
                    self.other.hit()
                    i.end()
                    self.hits += 1
                    self.writeHits()
                    break
            self.screen.ontimer(self.timeStep, 25)
    #rewrite how many times the enemy has been hit
    def writeHits(self):
        self.hitsTurtle.clear()
        self.hitsTurtle.write('Hits:  '+str(self.hits), align='right',
                              font=('Arial',16,'normal'))
    #update how much time has passed
    def updateTime(self):
        if not self.pause and not self.finished:
            self.time += .1 ###################previously 1
            self.writeTime()
            self.ontimer(self.updateTime, 100)######previously 1000
    #rewrite the counter for the time taken
    def writeTime(self):
        self.timeTurtle.clear()
        self.timeTurtle.write('Time:  '+str(int(self.time)), align='center', \
                              font=('Arial',16,'normal'))
    #do fun after milliseconds time has passed
    def ontimer(self, fun, milliseconds):
        self.screen.ontimer(fun, milliseconds)
    #bind fun to key in the game
    def onkey(self, fun, key):
        self.screen.onkey(fun, key)
    #bind fun to keyrelease in the game
    def onkeyrelease(self, fun, key):
        self.screen.onkeyrelease(fun, key)
    #bind fun to keypress in the game
    def onkeypress(self, fun, key):
        self.screen.onkeypress(fun, key)
    #rewrite the counter for the number of shots fired
    def writeShots(self):
        self.shotsFiredTurtle.clear()
        self.shotsFiredTurtle.write('Shots:  '+str(self.shotsFired), \
                                    font=('Arial',16,'normal'))
    #add a piece of ammo to the list for active shots
    def addAmmo(self, ammo):
        if self.finished:
            return
        self.liveAmmo.append(ammo)
        self.shotsFired += 1
        self.writeShots()
    #remove a piece of ammo from the list for active shots
    def removeAmmo(self, ammo):
        try:
            self.liveAmmo.remove(ammo)
        except:
            pass
    #mute/unmute game
    def toggleMute(self):
        self.mute = not self.mute
        self.character.toggleMute()
        self.other.toggleMute()
    #pause/unpause game
    def togglePause(self):
        self.pause = not self.pause
        self.character.togglePause()
        self.other.togglePause()
        for i in self.liveAmmo:
            i.togglePause()
        if self.pause:
            time.sleep(1) #make it so they can't move faster by toggling pause all the time
        else:
            self.timeStep()
            self.updateTime()
    #move hero to grab enemy immediately
    def cheat(self):
        if self.pause or self.finished:
            return
        self.cheatsUsed += 1
        self.end()
        x,y = self.other.pos()
        self.character.goto(x,y)
    #slow the enemy down a lot
    def nuke(self):
        if self.pause or self.finished:
            return
        self.cheatsUsed += 1
        self.other.nuke()
        self.writeTurtle.goto(0,0)
        self.writeTurtle.write('NUKED',align='center',font=('Arial',80,'bold'))
        self.ontimer(lambda: self.writeTurtle.clear(), 300)
    #finds the okay directions for the enemy to go without meeting hero
    def badEnemyDirection(self):
        ox,oy = self.other.pos()
        cx,cy = self.character.pos()
        good = [0, 90, 180, 270]
        dist = 40
        if abs(cx-ox)<dist and abs(cy-oy)<dist: #close
            if ox < cx:
                good.remove(0)
            else:
                good.remove(180)
            if oy < cy:
                good.remove(90)
            else:
                good.remove(270)
        return good
    #what to do when player wins game
    def end(self):
        self.finished = True
        self.character.finish()
        self.other.finish()
        if not self.mute:
            winsound.PlaySound('sounds\hit_with_frying_pan_y.wav', winsound.SND_FILENAME)
            winsound.PlaySound('sounds\yay_z.wav', \
                               winsound.SND_FILENAME|winsound.SND_ASYNC)
        for i in self.liveAmmo:
            i.end()
        #display messages
        y = 0
        self.writeTurtle.goto(0,y)
        self.writeTurtle.write('Got him!',align='center',font=('Arial',18,'bold'))
        y -= 20
        self.writeTurtle.goto(0,y)
        self.writeTurtle.write('Shots:  '+str(self.shotsFired), align='center',\
                               font=('Arial',14,'normal'))
        y -= 20
        self.writeTurtle.goto(0,y)
        self.writeTurtle.write('Hits:  '+str(self.hits), align='center',\
                               font=('Airal',14,'normal'))
        y -= 20
        self.writeTurtle.goto(0,y)
        if self.shotsFired != 0:
            percentage = int(self.hits/self.shotsFired*100)
        else:
            percentage = 100
        self.writeTurtle.write('Hit Percentage:  '+str(percentage),\
                               align='center', font=('Arial',14,'normal'))
        y -= 20
        self.writeTurtle.goto(0,y)
        self.writeTurtle.write('Time:  '+str(int(self.time)),\
                               align='center',font=('Arial',14,'normal'))
        if self.cheatsUsed > 0:
            y -= 20
            self.writeTurtle.goto(0,y)
            self.writeTurtle.write('Cheats Used:  '+str(self.cheatsUsed),\
                                   align='center',font=('Arial',14,'normal'))
        y -= 50
        self.writeTurtle.goto(0,y)
        self.writeTurtle.write('Press r to reset', align='center',
                               font=('Arial',14,'normal'))
        self.onkey(self.reset, "r")
    #get out of the game
    def Quit(self):
        try:
            self.screen.bye()
        except:
            pass



if __name__ == "__main__":
    import os
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    try:
        Game()
    except:
        pass
