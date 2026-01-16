from CSP_Cubo import *

localInts = 5000
localFloats = 7000
localStrings = 9000
localBools = 11000

tempInts = 13000
tempFloats = 15000
tempStrings = 17000
tempBools = 19000

basesMem = {
	'local': {
		'ints':{'base':localInts},
		'floats': {'base':localFloats},
		'strings': {'base':localStrings},
		'bools': {'base':localBools}
	},
	'temp': {
		'ints': {'base':tempInts},
		'floats': {'base':tempFloats},
		'strings': {'base':tempStrings},
		'bools': {'base':tempBools}
	}
}

class ExecutionStack:

	def __init__(self, idF, cantVars):
		self.nombre = idF


		self.mapaMem = {
			'local': {
				'ints': {},
				'floats': {},
				'strings': {},
				'bools': {}
			},
			'temp': {
				'ints': {},
				'floats': {},
				'strings': {},
				'bools': {}
			}
		}

		self.GeneraLocales2(cantVars)



	def GeneraLocales2(self, cantVar):
		for a in list(cantVar.keys()):
			if a == 'bT':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['temp']['bools'][cont-1] = None
					cont -=1
			elif a == 's':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['local']['strings'][cont-1] = None
					cont -=1
			elif a == 'b':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['local']['bools'][cont-1] = None
					cont -=1
			elif a == 'fT':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['temp']['floats'][cont-1] = None
					cont -=1
			elif a == 'f':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['local']['floats'][cont-1] = None
					cont -=1
			elif a == 'i':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['local']['ints'][cont-1] = None
					cont -=1
			elif a == 'iT':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['temp']['ints'][cont-1] = None
					cont -=1
			elif a == 'sT':
				cont = cantVar[a]
				while cont > 0:
					self.mapaMem['temp']['strings'][cont-1] = None
					cont -=1



	def getIdPrograma(self):
		return self.nombre

	def getMapaMem(self):
		return self.mapaMem

	def getValorMapaMem(self, scope, tipo, llave, direccion):
		valor = self.mapaMem[scope][tipo][llave]
		if valor == None:
			print("Error, variable con scope %s y de tipo %s con direccion %s no inicializada en nombre func: %s!" % (scope, tipo, direccion, self.nombre))
			print(self.mapaMem)
			exit()
		return valor

	def guardaValMapaMem(self, valor, scope, tipo, llave):
		if llave not in self.mapaMem[scope][tipo].keys():
			print("Error, no existe ese espacio para almacenar el valor!")
		else:
			self.mapaMem[scope][tipo][llave] = valor






