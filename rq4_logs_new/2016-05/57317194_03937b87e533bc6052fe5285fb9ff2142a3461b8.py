from threading import Thread

class Music():
	DLL = 'avbin64'
	def ryno () :
		import pyglet
		pyglet.lib.load_library(DLL)
		pyglet.have_avbin=True
		sound = pyglet.media.load('../assets/music/Rynos_Theme.mp3')
		sound.play()
		pyglet.app.run()
	
	def playRyno():
	    global player_thread
	    player_thread = Thread(target=Music.ryno)
	    player_thread.start()
	    
	
	def wizards () :
		import pyglet
		pyglet.lib.load_library(DLL)
		pyglet.have_avbin=True
		sound = pyglet.media.load('../assets/music/Final Battle of the Dark Wizards.mp3')
		sound.play()
		pyglet.app.run()
	
	def playWizards():
	    global player_thread
	    player_thread = Thread(target=Music.wizards)
	    player_thread.start()
	
	def city () :
		import pyglet
		pyglet.lib.load_library(DLL)
		pyglet.have_avbin=True
		sound = pyglet.media.load('../assets/music/Floating Cities.mp3')
		sound.play()
		pyglet.app.run()
	
	def playCity():
	    global player_thread
	    player_thread = Thread(target=Music.city)
	    player_thread.start()