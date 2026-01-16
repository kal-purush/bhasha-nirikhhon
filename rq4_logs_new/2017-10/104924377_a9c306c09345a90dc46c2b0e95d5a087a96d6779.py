from Map.tile_types import GrassTile, WaterTile
import random


class GameMap:
	tile_size = 50
	tiles = [GrassTile, WaterTile]

	def create_new_map(self):
		new_map = []
		for x in range(self.width):
			new_map.append([])
			row = new_map[x]
			for y in range(self.height):
				row.append([])
				row[y] = self.new_random_tile(x, y, GameMap.tile_size)
		return new_map

	def new_random_tile(self, x, y, tile_size):
		tile_number = random.randint(0, len(GameMap.tiles) - 1)
		return GameMap.tiles[tile_number](x*tile_size, y*tile_size, tile_size, tile_size)

	def draw(self, screen):
		screen.fill((0, 0, 0))

		for x in range(self.width):
			for y in range(self.height):
				self.map[x][y].draw(screen)

	def __init__(self, width=10, height=10):
		self.width = width
		self.height = height
		self.map = self.create_new_map()