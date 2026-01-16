from Entity import Entity
import math

class Anthony(Entity):
	def __init__(self, minX, maxX, minY, maxY):
		super(Anthony, self).__init__(minX, (minY + maxY) / 2, 1, "textures/anthony.png", [1, 1, 1, 1, 10, 10, 10, 10, 3], 20)
		self.anim = 8
		self.xEvol = 1
		self.animtemp = 0
		self.minX = minX
		self.maxX = maxX
		self.minY = minY
		self.maxY = maxY

	def render(self, window, camera):
		self.position = self.position.move(self.xEvol, (self.maxY - self.minY) / 2 * math.sin(self.animtemp / 30) + (self.minY + self.maxY) / 2 - self.position.y)
		self.animtemp += 1
		if self.animtemp >= 30 * 2 * math.pi:
			self.animtemp -= 30 * 2 * math.pi
		if self.position.x <= self.minX or self.position.x >= self.maxX:
			self.xEvol = -self.xEvol
		super(Anthony, self).render(window, camera)
		if self.anim == 0:
			self.anim = 8