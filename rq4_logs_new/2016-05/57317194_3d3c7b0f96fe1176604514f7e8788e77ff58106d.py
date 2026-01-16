import PTAG

class RoomVars():
	found_Stone=False
	doorUnlocked=False
	found_Key=False
	
class ButtonLabel():
	font = PTAGVars.ButtonFont[0]
	size = PTAGVars.ButtonFont[1]
	#Game Name and Start Button Labels
	GameTitle = pyglet.text.Label("Python Text Adventure Game", font_name=font, font_size=size, x=window.width//2, y=window.width//2, anchor_x='center', anchor_y='top', color=(255,0,0,255))
	StartLabel = pyglet.text.Label("Click to Start", font_name=font, font_size=size, x=(window.width//2)-20, y=(window.width//2)-100, anchor_x='center', anchor_y='center')
	
	#Player Statistics
	playerNameLabel = pyglet.text.Label(PTAGVars.player[0], font_name=font, font_size=size, x=window.width, y=window.height, anchor_x='right', anchor_y='top')
	playerHealthLabel = pyglet.text.Label(PTAGVars.playerHealth, font_name=font, font_size=size, x=window.width, y=(window.height-15), anchor_x='right', anchor_y='top')
	playerResistanceLabel = pyglet.text.Label(PTAGVars.playerResistance, font_name=font, font_size=10, x=window.width, y=(window.height-30), anchor_x='right', anchor_y='top')
	playerArmourLabel = pyglet.text.Label(PTAGVars.playerArmour, font_name=font, font_size=10, x=window.width, y=(window.height-45), anchor_x='right', anchor_y='top')
	playerLocationLabel = pyglet.text.Label(PTAGVars.playerLocation, font_name=font, font_size=10, x=window.width, y=(window.height-60), anchor_x='right', anchor_y='top')
	
	#General Game Information Labels
	missionObjectiveLabel = pyglet.text.Label(PTAGVars.missionObjective, font_name='Arial Rounded MT Bold', font_size=10, x=window.width//2 ,y=0, anchor_x='center', anchor_y='bottom')
	storyLabel = pyglet.text.Label('', font_name='Arial', font_size=10, x=window.width//2, y=window.height//2, anchor_x='center', anchor_y='center')
	speaker = pyglet.image.load('../assets/images/Speaker.png')
	
	#Debug Labels
	pointerClickLocation = pyglet.text.Label('0, 0', font_name='Arial', font_size=10, x=window.width, y=0, anchor_x='right', anchor_y='bottom')
	fps = pyglet.clock.ClockDisplay()
	
	def inBounds(bounds, pointer):
		if pointer[0] <= bounds[2] and pointer[0] >= bounds[0] and pointer[1] <= bounds[1] and pointer[1] >= bounds[3]:
			return(True)
	
	#Buttons
	#Look Around Button
	LookAroundButtonLabel = pyglet.text.Label('Look Around', font_name=font, font_size=10, x=10, y=300, anchor_x='left', anchor_y='center')
	def lookAround():
		pointer = PTAGVars.mouseCoords
		#Button Bounds:
		bounds = (10, 305, 89, 295)#Top Left, Bottom Right
		if ButtonLabel.inBounds(bounds, pointer):
			ButtonLabel.storyLabel.text='There is not much to see here'
			print("There is not much to see here")
		
	#Listen Closely Button
	ListenCloselyLabel = pyglet.text.Label('Listen Closely', font_name=font, font_size=10, x=10, y=285, anchor_x='left', anchor_y='center')
	def listenClosely():
		pointer = PTAGVars.mouseCoords
		#Button Bounds
		bounds = (9, 290, 89, 280)
		if ButtonLabel.inBounds(bounds, pointer):
			print("You hear muffled noises that could be voices or music.")
			ButtonLabel.storyLabel.text='You hear muffled noises that could be voices or music'
	
	#Feel Around Button
	FeelAroundLabel = pyglet.text.Label('Feel Around', font_name=font, font_size=10, x=10, y=270, anchor_x='left', anchor_y='center')
	def feelAround():
		pointer = PTAGVars.mouseCoords
		#Button Bounds
		bounds = (9, 275, 89, 265)
		if ButtonLabel.inBounds(bounds, pointer):
			ButtonLabel.storyLabel.text='You find a door and a loose stone.'
			RoomVars.found_Stone=True
			print('You find a door and a loose stone.')
	
	#Try Door Button
	TryDoorLabel = pyglet.text.Label('Try the door', font_name=font, font_size=10, x=10, y=270, anchor_x='left', anchor_y='center')
	def tryDoor():
		pointer = PTAGVars.mouseCoords
		#Button Bounds
		bounds = (10, 275, 78, 266)
		if ButtonLabel.inBounds(bounds, pointer):
			if RoomVars.found_Key:
				print("The door opens smoothly and quietly.")
				ButtonLabel.storyLabel.text='The door opens smoothly and quietly.'
				PTAG.endRoom()
			else:
				print("The door is locked. You must find a key before it will open.")
				ButtonLabel.storyLabel.text='The door is locked. You must find a key before it will open.'
	
	#Try Stone Button
	TryStoneLabel = pyglet.text.Label('Look under the stone', font_name=font, font_size=size, x=100, y=270, anchor_x='left', anchor_y='center')
	def tryStone():
		pointer = PTAGVars.mouseCoords
		#Button Bounds
		bounds = (97, 275, 218, 266)
		if ButtonLabel.inBounds(bounds, pointer):
			print('You find a key underneath the stone. It might go to a door')
			ButtonLabel.storyLabel.text='You find a key underneath the stone. It might go to a door.'
			RoomVars.found_Key = True
			
			
class RoomOne():
	print('Hi')