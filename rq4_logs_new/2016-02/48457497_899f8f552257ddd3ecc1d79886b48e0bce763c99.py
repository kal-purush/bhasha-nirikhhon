class Song(object):

	def __init__(self, lyrics):
		self.lyrics = lyrics
		
	def sing_me_a_song(self):
		for line in self.lyrics:
			print line
		print "\n"
		
	amazing_grace = ("""\tAmazing grace, how sweet the sound 
	That saved a wretch like me
	I once was lost, but now am found
	Was blind, but now I see""")
			
happy_bday = Song(["Happy birthday to you",
				"I don't want to get sued",
				"So I'll stop right there"])
					
bulls_on_parade = Song(["They rally round tha family",
						"With pockets full of shells."])
						
jesus_loves_me = Song(["Jesus loves me this I know",
						"For the Bible tells me so",
						"Little ones to Him belong",
						"They are weak, but He is strong"])
						
happy_bday.sing_me_a_song()

bulls_on_parade.sing_me_a_song()

jesus_loves_me.sing_me_a_song()

amazing_G = Song

print amazing_G.amazing_grace