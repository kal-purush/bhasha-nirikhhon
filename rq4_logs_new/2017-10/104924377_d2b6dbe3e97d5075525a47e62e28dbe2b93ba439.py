import pygame
from Map.MapTile import MapTile
from Managers import GameManager

class Unit:

	radius = int(MapTile.tile_width / 3)

	def __init__(self, x, y):
		self.x = self.targetX = x
		self.y = self.targetY = y

	def go_to(self, targetX, targetY):
		if targetX > -1 and targetX < GameManager.board_width():
			self.targetX = targetX
		if targetY > -1 and targetY < GameManager.board_height():
			self.targetY = targetY

	def move(self):
		if self.x < self.targetX:
			self.x += 1
		elif self.x > self.targetX:
			self.x -= 1

		if self.y < self.targetY:
			self.y += 1
		elif self.y > self.targetY:
			self.y -= 1

	def get_screen_loc(self):
		return (self.x*MapTile.tile_width + int(MapTile.tile_width/2), self.y*MapTile.tile_height + int(MapTile.tile_height/2))

	def draw(self, screen):
		pygame.draw.circle(screen, (0, 0, 0), self.get_screen_loc(), self.radius)

	def update(self, screen):
		self.move()
		# TODO check to make sure we are inbounds here
		self.draw(screen)