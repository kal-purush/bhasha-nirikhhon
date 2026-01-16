from random import randrange

class Collision:
	def __init__(self):
		self.collision_occurred: str = "Y" if randrange(0,2) == 1 else "N"
 
		# Generate collision point
		self.collision_point: int = randrange(0,8)

	def get_point_string(id: int) -> str:
		match id:
			case 0:
				return "CL1"
			case 1:
				return "CL2"
			case 2:
				return "CL3"
			case 3:
				return "CL4"
			case 4:
				return "CL5" 
			case 5:
				return "CP1"
			case 6:
				return "CS1"
			case 7:
				return "CS2"
		return "None"

	def __repr__(self):
		return f"{self.collision_occurred}, {Collision.get_point_string(self.collision_point)}"
	
	def get_headers():
		return "Collision, Collision_point"