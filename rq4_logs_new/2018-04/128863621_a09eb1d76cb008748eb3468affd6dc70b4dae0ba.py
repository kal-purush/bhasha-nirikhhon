from engine import *
from color import Color
import random
import pygame

class My_World(World):
	def __init__(self):
		super().__init__(800, 600)
		self._gravity = vector(m=-1, r=270)
		self.add_sprites(Normal_Entity((0,595), (800,5), Color.white))

	def update(self):
		if self.key('any'):
			self.add_sprites(Box(*pygame.mouse.get_pos()))

		for entity in self._all_sprites:
			if entity.get_y() > self.get_height():
				self.remove_sprites(entity)

		super().update()

class Box(Physics_Entity):
	def __init__(self, x, y):
		super().__init__((x, y), (10, 10), 10)

	def update(self):
		#print('x: ' + str(self.get_x()) + ' y: ' + str(self.get_y()))
		#print('rect: ' + str(self.rect))
		#print('color: ' + str(self._color))

		if self.get_world().key('left arrow') and not self.exist_force('left'):
			self.add_force('left', (60, 180))
			self.remove_force('right')
			self.remove_force('up')
			self.remove_force('down')
		elif self.get_world().key('right arrow') and not self.exist_force('right'):
			self.add_force('right', (60, 0))
			self.remove_force('left')
			self.remove_force('up')
			self.remove_force('down')
		elif self.get_world().key('up arrow') and not self.exist_force('up'):
			self.add_force('up', (-60, 90))
			self.remove_force('left')
			self.remove_force('right')
			self.remove_force('down')
		elif self.get_world().key('down arrow') and not self.exist_force('down'):
			self.add_force('down', (-20, -90))
			self.remove_force('left')
			self.remove_force('right')
			self.remove_force('up')
		else:
			self.remove_force('left')
			self.remove_force('right')
			self.remove_force('up')
			self.remove_force('down')

		super().update()

if __name__ == '__main__':
	my_world = My_World()

	engine = Engine(my_world, 'Test Game', 30)
	engine.start()