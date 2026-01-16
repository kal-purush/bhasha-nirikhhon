#Nhan Le and Zong Lim
#CS111 Final Project
#DotvsBlock
#Game that involves controlling the player object with horizontal arrow keys at the bottom of the screen to collect points in the form of falling circles. Special objects entail differnt effects.

import pygame, sys
from random import *

def getRandColor():
	""" returns an rgb tuple with random values between 0 and 255 """
	return (randrange(0,256), randrange(0,256), randrange(0,256))

class dotVsBlock:
	""" overall game class """

	def __init__(self, winWidth, winHeight, numCirc, quick, slow, plus):
		""" game constructor. Takes argument:
			width of game window,
			height of game window
			number of circles
			file for quickOb
			file for slowOb
			file for plusOb """
		# initialize the player
		self.player = player(16, [51,255,51], [300,550], (0, winWidth))
		self.numCirc = numCirc
		self.circles = []
		# initialize circles with different radii, colors, start position, and speeds
		# all boxes have same bounds (width and height of window)
		for i in range(numCirc): 
			radius = randrange(10,25)
			color = getRandColor()
			a = randrange(0,winWidth-2*(radius))
			pos = (a, 0 - a)
			speed = (randrange(1,5))
			newCirc = dropCirc(radius, color, pos, speed, (0, winWidth), (0, winHeight))
			self.circles.append(newCirc)
		#initialize the special objects
		imageList = [quick, slow, plus]
		self.special = []
		for i in range(3):
			speed = randrange(3, 7)
			imageL = pygame.image.load(imageList[i])
			measure = imageL.convert()
			imW = measure.get_width() 
			imH = measure.get_height()
			direction = [choice([-1,1]), 1]
			newOb = specialOb(imageL, imW, imH, [randrange(0, winWidth - imW), randrange(-winHeight + 1, winHeight - imH)], speed, direction, (0, winWidth), (-winHeight, winHeight), i + 1)
			self.special.append(newOb)
		self.lives = 3
		self.playerMoving = False
		self.points = 0
		self.seconds = 0
		self.minutes = 0

	def gameOver(self):
		""" game ends if have no more lives """
		return self.lives == 0

	def endScreen(self, screen):
		""" print message, final score, and time to game window when player loses/dies """
		myfont = pygame.font.SysFont("Calibri", 69)
		endLine1 = myfont.render("GAME OVER", 1, (0, 0, 0))
		myfont = pygame.font.SysFont("Arial", 39)
		endLine2 = myfont.render("Final Score: " + str(self.points), 1, (0, 0, 0))
		myfont = pygame.font.SysFont("Arial", 30)
		printTime = myfont.render("TIME: " +str(self.minutes) + ":" + str(self.seconds), 1, (0,0,0))
		if self.seconds < 10:
			printTime = myfont.render("TIME: " +str(self.minutes) + ":0" + str(self.seconds), 1, (0,0,0))
		screen.fill((195,0,0))
		screen.blit(endLine1, (150, 200))
		screen.blit(endLine2, (175, 260))
		screen.blit(printTime, (210, 320))
		pygame.display.flip()

	def handleEvent(self, event, winWidth, winHeight):
		""" checks for left/right arrow key press/release, player and object/circle interaction, and takes appropriate action for each """ 
		# if horizontal arrow keys are pressed, set moving to True, and the appropriate direction
		keys = pygame.key.get_pressed()
		if event.type == pygame.KEYDOWN:
			if keys[pygame.K_RIGHT] or keys[pygame.K_LEFT]:
				self.playerMoving = True
				self.player.dir = 1
				if keys[pygame.K_LEFT]:
					self.player.dir *= -1
		
		# if no keyboard input, no movement for player
		if event.type == pygame.KEYUP:
			self.playerMoving = False

		# if the player presses spaces
		if event.type == pygame.KEYDOWN and event.key == pygame.K_SPACE:
		# check if player has reached a circle; if circle reaches the bottom of the screen, reset the circle
			miss = True
			pointGain = 0
			for circle in self.circles:
				if (circle.pos[0] - circle.radius - self.player.length <= self.player.pos[0] <= circle.pos[0] + circle.radius)	\
					and (circle.pos[1] - circle.radius <= self.player.pos[1] <= circle.pos[1] + circle.radius):
					pointGain += circle.point
					circle.radius = randrange(10,25)
					circle.color = getRandColor()
					a = randrange(0,winWidth-2*(circle.radius))
					circle.pos[0] = a
					circle.pos[1] = 0 - a
					circle.speed = (randrange(1,5))
					miss = False
			#check if player reached a special object; if yes, reset the position and speed of the special object
			for obJ in self.special:
				if (obJ.pos[0] - obJ.width) <= self.player.pos[0] <= (obJ.pos[0] + obJ.width) \
				  and (obJ.pos[1] - obJ.height) <= self.player.pos[1] <= (obJ.pos[1] + obJ.height):
					a = randrange(0,winWidth - obJ.width)
					b = randrange(-winHeight, - obJ.height +1)
					obJ.pos[0] = a
					obJ.pos[1] = b
					obJ.speed = randrange(3,7)
					if obJ.type == 1:
						self.player.speed *= 3
					if obJ.type == 2:
						self.player.speed //= 3
						if self.player.speed < 1:
							self.player.speed = 1
					if obJ.type == 3:
						self.lives += 1
					miss = False

			#if player misses, deduct a life
			if miss:
				self.lives -= 1
			else:
				self.points += pointGain
				
		# if arrow key is pressed, update the postion for the player
		if self.playerMoving:
			self.player.updatePos()

		# if user presses "p", pause the game until pressed again; cannot quit game until game is resumed
		if event.type == pygame.KEYDOWN and event.key == pygame.K_p:
			event = pygame.event.poll()
			while not (event.type == pygame.KEYDOWN and event.key == pygame.K_p):
				event = pygame.event.poll()
		
	
	def draw(self, screen, winWidth, winHeight):
		""" draws the player, circles, special objects, time, points and num lives """
		#draws player
		self.player.draw(screen)
		#draws circle
		for circle in self.circles:
		# to make the circle fall constant
			if circle.pos[1] > winHeight + circle.radius: 
				circle.radius = randrange(10,25)
				circle.color = getRandColor()
				a = randrange(0,winWidth-2*(circle.radius))
				b = randrange(-2*circle.radius, 0)
				circle.pos[0] = a
				circle.pos[1] = b
				circle.speed = (randrange(1,5))
			circle.draw(screen)
		#draws special object
		for iM in self.special:
			image = iM.input()
			screen.blit(image, (iM.pos[0], iM.pos[1]))
		myfont = pygame.font.SysFont("Times New Roman", 22)
		lives = myfont.render("LIVES: "+str(self.lives), 1, (51, 255, 51))
		points = myfont.render("POINTS: "+str(self.points), 1, (51, 255, 51))
		seconds, minutes = (pygame.time.get_ticks()//1000), (pygame.time.get_ticks()//60000)
		printTime = myfont.render("TIME: " +str(minutes) + ":" + str(seconds), 1, (102,178,255))
		if seconds < 10:
			printTime = myfont.render("TIME: " +str(minutes) + ":0" + str(seconds), 1, (102,178,255))
		elif seconds >= 60:
			seconds -= 60*(minutes)
			if seconds < 60:
				printTime = myfont.render("TIME: " +str(minutes) + ":" + str(seconds), 1, (102,178,255))
			if seconds < 10:
				printTime = myfont.render("TIME: " +str(minutes) + ":0" + str(seconds), 1, (102,178,255))
		self.minutes = minutes
		self.seconds = seconds
		screen.blit(printTime, (250,20))
		screen.blit(lives, (20,20))
		screen.blit(points, (466,20))

	def moveCirc(self):
		""" moves all of the circles 1 step """
		for circle in self.circles:
			circle.moveStep()

	def moveSpecialOb(self):
		""" moves all special object 1 step """
		for obJ in self.special:
			obJ.moveStep()

class specialOb:
	""" class to represent the sepcial object that alter the movement of the players """
	
	def __init__(self, image, width, height, initPos, speed, direction, boundsX, boundsY, typeO):
		""" special object constructor. Takes argument:
			image of the object
			width of the object
			height of the object
			tuple or list with x,y coordinates of initial position
			speed of the object
			direction of the object
			tuple or list with min,max allowed x coordinates for the box to occupy
			tuple or list with min,max allowed y coordinates for the box to occupy
			type """
		self.image = image
		self.width = width
		self.height = height
		self.pos = list(initPos)
		self.speed = speed
		self.dir = direction
		self.boundsX = boundsX
		self.boundsY = boundsY
		self.type = typeO
		
	def input(self):
		""" returns image on the screen """
		return self.image

	def moveStep(self):
		""" moves the image one step in its current direction, unless it hits the player """
		if self.pos[0] < self.boundsX[0] or \
			self.pos[0] > (self.boundsX[1] - self.width):
				self.dir[0] *= -1
		if self.pos[1] < self.boundsY[0] or \
		  self.pos[1] > (self.boundsY[1] - self.height):
				self.dir[1] *= -1
			
		self.pos[0] += self.dir[0]*self.speed
		self.pos[1] += self.dir[1]*self.speed

class dropCirc: 
	""" class that represents a simple circle that drops from the top of the screen """
	
	def __init__(self, radius, color, initPos, speed, boundsX, boundsY):
		""" Dropping circle constructor. Takes arguments: 
			radius of the circle
			color to fill the circle
			tuple or list with x,y coordinates of initial position
			speed at which circle will move (how many pixels per step)
			tuple or list with min,max allowed x coordinates for the circle to occupy
			tuple or list with min,max allowed y coordinates for the circle to occupy """
		
		self.radius = radius
		self.pos = list(initPos)
		self.color = color
		self.speed = speed
		self.dir = [choice([-1,1]),1]
		self.boundsX = boundsX
		self.point = radius//3

	def draw(self, screen):
		""" draws the circle to the screen """
		pygame.draw.circle(screen, self.color, self.pos, self.radius)

	def moveStep(self):
		""" moves the circle one step in its current direction, unless it hits the bottom 
			or player, in which case resets """
		if self.pos[0] <= self.boundsX[0] or \
		(self.pos[0]+ 2*(self.radius)) >= self.boundsX[1]:
			self.dir[0] *= -1
			
		self.pos[0] += self.dir[0]*self.speed
		self.pos[1] += self.dir[1]*self.speed

class player:
	""" class to represent the object controlled by the player """
	
	def __init__(self, length, color, initPos, boundsX):
		""" player's object constructor. Takes argument:
			length of box
			color to draw box
			tuple or list with x,y coordinates of initial position
			tuple or list with min,max allowed x coordinates for the box to occupy
			tuple or list with min,max allowed y coordinates for the box to occupy
			direction """

		self.length = length
		self.color = color
		self.pos = list(initPos)
		self.boundsX = boundsX
		self.dir = 1
		self.speed = 6

	def draw(self,screen):
		""" draw the player """
		pygame.draw.rect(screen, self.color, (self.pos[0], self.pos[1], self.length, self.length), 3)

	def updatePos(self):
		""" move player based on the arrow keys while setting a limit """
		keys = pygame.key.get_pressed()
		if self.pos[0] >= (self.boundsX[1] - self.length):
			if keys[pygame.K_RIGHT]:
				self.dir = 0
			else:
				self.dir = -1
		elif self.pos[0] <= self.boundsX[0]:
			if keys[pygame.K_LEFT]:
				self.dir = 0
			else:
				self.dir = 1

		self.pos[0] += self.dir*self.speed
		if self.pos[0] <= self.boundsX[0]:
			self.pos[0] = self.boundsX[0]
		if self.pos[0] >= self.boundsX[1] - self.length:
			self.pos[0] = self.boundsX[1] - self.length

		 
def main():
	""" plays DotVsBlock game!!! """
	
	# sets up all pygame modules for use
	pygame.init()

	#sets up background music to loop infinitely
	pygame.mixer.music.load("music.wav")
	pygame.mixer.music.play(-1)

	# set up window with given width, height, and caption
	winWidth, winHeight = 600, 600
	screen = pygame.display.set_mode((winWidth, winHeight))
	pygame.display.set_caption("DOT VS BLOCK!!!")

	# initialize parameters and draw just background screen
	screen.fill((0,0,0))

	# create a new Dot vs Block with a constant fall of circle, special objects, and details
	game = dotVsBlock(winWidth, winHeight, 10, "star.png", "snail.png","plus.png")
	game.draw(screen, winWidth, winHeight)

	# add text box for instructions before the game starts
	insts = ["Press the left/right arrow key to move", 
		"the green box. Press space when the circle is on top",
		"of the green box to gain points!", "Empty space = -life", "Bigger circles are worth more points!"]
	myfont = pygame.font.SysFont("Calibri", 26)
	instsLine1 = myfont.render(insts[0], 1, (102, 178, 255))
	instsLine2 = myfont.render(insts[1], 1, (102, 178, 255))
	instsLine3 = myfont.render(insts[2], 1, (102, 178, 255))
	instsLine4 = myfont.render(insts[3], 1, (255, 0, 255))
	instsLine5 = myfont.render(insts[4], 1, (255, 0, 255))
	instsLine6 = myfont.render("Star = FAST", 1, (0, 255, 0))
	instsLine7 = myfont.render("Snail = SLOW" , 1, (0, 255, 0))
	instsLine8 = myfont.render("Plus = +LIFE" , 1, (0, 255, 0))
	instsLine9 = myfont.render("Hit p at any time to pause.", 1, (255, 255, 0))
	myfont = pygame.font.SysFont("Calibri", 33)
	instsLine10 = myfont.render("PRESS LEFT/RIGHT ARROW KEY TO BEGIN", 1, (0, 238, 238))
	screen.blit(instsLine1, (50, 100))
	screen.blit(instsLine2, (50, 125))
	screen.blit(instsLine3, (50, 150))
	screen.blit(instsLine4, (50, 175))
	screen.blit(instsLine5, (50, 200))
	screen.blit(instsLine6, (250, 225))
	screen.blit(instsLine7, (250, 250))
	screen.blit(instsLine8, (250, 275))
	screen.blit(instsLine9, (175, 300))
	screen.blit(instsLine10, (65,330))
	pygame.display.flip()

	# create the graphics clock
	clock = pygame.time.Clock()

	# continuously poll to see if the user has pressed any key	
	# check the specific event.key to see if it was the left/right arrow key.
	event = pygame.event.poll()
	while not (event.type == pygame.KEYDOWN and (event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT)):
		# checks for the close button in the window being pressed
		if event.type == pygame.QUIT:
			quit()
		clock.tick(50)
		event = pygame.event.poll()

	# Now continue until user clicks x to close window
	while event.type != pygame.QUIT:
		
		# start by "erasing" everything by painting screen with background color
		screen.fill((0,0,0))

		# deal with any user input keypress that has occurred since last frame
		game.handleEvent(event, winWidth, winHeight)

		# if last life was just lost, quit game loop
		if game.gameOver():
			game.endScreen(screen)
			break

		# move the circles one step each
		game.moveCirc()

		# move the special objects one step each
		game.moveSpecialOb()
		
		# draw the circles, player, special objects, lives, time, and points at the updated position
		game.draw(screen, winWidth, winHeight)

		# show the new screen drawn
		pygame.display.flip()

		# sets frames/second so that game speed so will be same on all computers
		clock.tick(30)

		# get next event
		event = pygame.event.poll()

	# Keep showing game over screen until user clicks x to close window
	while event.type != pygame.QUIT:
		event = pygame.event.poll()


if __name__=="__main__":
	main()