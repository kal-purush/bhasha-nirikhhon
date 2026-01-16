import random

		
class Armor(object):
	
	def __init__(self):
		self.protection = 0
		self.armor = "none"
	
	def assign(self):
		self.armorTypes = ["chain mail", "leather vest", "metal chest plate", "helmet"]
		armorAssign = self.armorTypes[random.randint(0,3)]
	
		if armorAssign == "chain mail":
			self.protection = random.randint(10, 15)
			self.armor = armorAssign
		elif armorAssign == "leather vest":
			self.protection = random.randint(5, 12)
			self.armor = armorAssign
		elif armorAssign == "metal chest plate":
			self.protection = random.randint(15, 24)
			self.armor = armorAssign
		else:
			self.protection = random.randint(7, 15)
			self.armor = armorAssign