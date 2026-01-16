from Map.MapTile import MapTile
from Map.GrassTile import GrassTile
from Map.WaterTile import WaterTile
import random

class MapManager:
	
	tiles = [GrassTile, WaterTile]

	def	create_new_map(self):
		new_map = []
		for x in range(self.width):
			new_map.append([])
			row = new_map[x]
			for y in range(self.height):
				row.append([])
				row[y] = self.new_random_tile(x, y)
		return new_map

	def new_random_tile(self, x=-1, y=-1):
		return MapManager.tiles[random.randint(0, len(MapManager.tiles)-1)](x, y)

	def draw(self, screen, to_draw=None):
		if to_draw == None: to_draw = self.map
		screen.fill((0, 0, 0)) # Black out the screen first

		for x in range(self.width):
			for y in range(self.height):
				self.map[x][y].draw(screen)

	def __init__(self, width=10, height=10):
		# Do nothing... for now
		self.width = width
		self.height = height
		self.map = self.create_new_map()