#!/usr/bin/python3


class Board(object):
	
	def __init__(self):

		initial_beads = 4

		self.gumot = {}
		for i in range(1,13): # Twelve gumot, six per player
			self.gumot[i] = initial_beads

		self.kupot = {
			0: 0,
			1: 0
		}


	def display(self):

		g = self.gumot
		k = self.kupot

		print('+======+======+======+======+======+======+======+======+')

		print('+=    =', end='')
		for i in range(7,13):
			print('+= %02d =' %g[i], end='')
		print('+=    =+')

		print('+= %02d =+======+======+======+======+======+======+= %02d =+' % (k[0],k[1],))

		print('+=    =', end='')
		for i in range(6,0,-1):
			print('+= %02d =' % g[i], end='')
		print('+=    =+')

		print('+======+======+======+======+======+======+======+======+')

		return True

b = Board()
b.display()