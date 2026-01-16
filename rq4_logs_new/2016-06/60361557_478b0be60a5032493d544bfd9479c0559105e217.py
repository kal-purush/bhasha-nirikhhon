class Song(object):
	
	def __init__(self, lyrics):
		self.lyrics = lyrics
	
	def sing(self):
		for line in self.lyrics:
			print line
	
bday = Song(raw_input("Enter a String"))
happ = Song(raw_input("Enter b String"))
happ.sing()
bday.sing()
