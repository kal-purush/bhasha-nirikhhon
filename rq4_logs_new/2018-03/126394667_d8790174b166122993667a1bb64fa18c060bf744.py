import Entity
class Bot:
	def __init__(self, xposition, yposition, xsize, ysize, symbol):
		Entity.Entity.__init__(self, [xposition, yposition, xsize, ysize], symbol)
		self.charge = 100
		self.TargetPosition = []
		self.Path = []
		
