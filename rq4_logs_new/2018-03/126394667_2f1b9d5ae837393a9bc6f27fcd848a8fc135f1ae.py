import math
class Landfill:
	def __init__(self, TBots, DBots, CStations, Pockets, Obstacles, Outline)
		self.xmax = 0
		self.xmin = 0
		self.ymax = 0
		self.ymin = 0
		for corner in Outline:
			self.xmax = max(self.xmax, corner[0])
			self.xmin = min(self.xmin, corner[0])
			self.ymax = max(self.ymax, corner[1])
			self.xmin = min(self.ymin, corner[1])
		self.xmax = self.xmax - self.xmin
		self.ymax = self.ymax - self.ymin
		for corner in Outline:
			corner[0]-=self.xmin
			corner[1]-=self.ymin
		self.shape = [[0 for col in range(self.xmax)] for row in range(slef.ymax)]
		for i in range(len(Outline)):
			self.shape[Outline[i][0]][Outline[i][1]] = 1;
			if i != 1:
				x = Outline[i][0]
				Dx = PreviousCorner[0]-Outline[i][0]
				y = Outline[i][1]
				Dy = PreviousCorner[1]-Outline[i][1]
				if Dx > Dy:
					dx = 1
					dy = Dy/Dx
					n = Dx
				else :
					dx = Dx/Dy
					dy = 1
					n = Dy
				for j in range(n):
					x += j*dx
					y += j*dy
					self.shape[trunc(x)][trunc(y)]
			PreviousCorner = Outline[i]
		self.TrackingBots = []
		for position in TBots:
			self.TrackingBots.append(Tracking_Bot(position[0]-self.xmin, position[1]-self.ymin))
		self.DrillingBots = []
		for position in DBots:
			self.DrillingBots.append(Drilling_Bot(position[0]-self.xmin, position[1]-self.ymin))
		self.ChargingStations = []
		for position in CStations:
			self.ChargingStations.append(Charging_Station(position[0]-self.xmin, position[1]-self.ymin))
		self.GasPockets = Pockets
		for pocket in self.GasPocket:
			pocket[0]-=self.xmin
			pocket[1]-=self.ymin
		
		