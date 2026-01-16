# SensorProcessor.py
# Gesture Recognition Algorithm.
# This version of SensorProcessor uses NEAT to identify the outputs

import time
import neat
import numpy as np
#global constants
global SENSOR_COUNT, UPDATE_TIME, VALID_GESTURE_TIME, BAUDRATE
SENSOR_COUNT = 6
UPDATE_TIME = 10
VALID_GESTURE_TIME = 0.01
BAUDRATE = 9600

def softmax(y):
	z = np.exp(y)
	return z / np.sum(z)

class Symbol:

	def __init__(self, ranges, data):
	   self.data = data
	   self.current_values = []
	   self._likelihood = 0.0
	   
	@property
	def likelihood(self):
		return self._likelihood
		   
	@likelihood.setter
	def likelihood(self, l):
		assert (l >= 0.0 and l <= 1.0)   
		self._likelihood = l	
	   
	def __str__(self):
		return self.Data
		
	def Update(self, values):
		self.current_values = values 
		
	def __le__(self, other):
		return self.likelihood <= other.likelihood	

class SymbolState:
	IDLE, VALID_GESTURE = range(2)

	def __init__(self):
		self.CurrentState = SymbolState.IDLE
		self.TimeInside = 0                      # time inside current state

	def SetState(self, newState, dt):

		if self.CurrentState != newState:        # enter new state => reset time
			self.TimeInside = 0
		else:                                    # already in new state => accumulate time
			self.TimeInside += dt

		self.CurrentState = newState


class SymbolManager:

	def __init__(self, symbols):

		self.Symbols = symbols
		self.State = SymbolState()
		self.ActivationData = None
		self.StartTime = time.time()
		self.Subscribers = []
		self.symbol_inputs = []
		self.symbol_outputs = []
		
		# Configure NEAT
		
		# Load configuration.
		self.config = neat.Config(neat.DefaultGenome, neat.DefaultReproduction,
			neat.DefaultSpeciesSet, neat.DefaultStagnation,
			'config-feedforward')
		
		# Create the population, which is the top-level object for a NEAT run.
		self.population = neat.Population(self.config)
		
		# Add a stdout reporter to show progress in the terminal.
		self.population.add_reporter(neat.StdOutReporter(False))
		
		# Winner Genome
		self.winner = self.population.run(self.EvalGenomes)
		self.net = neat.nn.FeedForwardNetwork.create(winner, config)
		
	def EvalGenomes(self, genomes, config):
		for genome_id, genome in genomes:
			genome.fitness = 4.0
			net = neat.nn.FeedForwardNetwork.create(genome, config)
			for x,y in zip(self.symbol_inputs, self.symbol_outputs):
				output = net.activate(x)
				genome.fitness -= (output[0] - y[0]) ** 2

	def Update(self, values):         # values = SENSOR_COUNT-element array of sensor feedback

		for i in range(0, len(self.Symbols)):
			self.Symbols[i].Update(values)

		flag = self.GetActivated(values)
		
		dt = time.time() - self.StartTime

		# symbol with maximum likelihood is identified
		
		if not flag:
			self.State.SetState(SymbolState.IDLE, dt)
			self.ActivationData = None
		
		else:
			self.State.SetState(SymbolState.VALID_GESTURE, dt)

			if self.State.TimeInside >= VALID_GESTURE_TIME:         # gesture identified:
				# the only element of actives array
				max_likelihood = 0
				identifiedGesture = None
				for s in self.Symbols: #estimate based on maximum likelihood
					if s.likelihood >= max_likelihood:
						identifiedGesture = s

				if self.ActivationData is None:                     # first time data change after IDLE state
					self.OnActivationChanged(identifiedGesture.Data)

				self.ActivationData = identifiedGesture.Data

		self.StartTime = time.time()

	def OnActivationChanged(self, data):
		for subscriber in self.Subscribers:
			subscriber(data)

	def GetActivated(self, values):
		# feeds the nn and applies softmax operation
		output = softmax(self.net.activate(values))
		
		#updates likelihoods after feeding to nn
		for i in range(len(self.Symbols)):
			self.Symbols[i].likelihood = output[i]
		
		return True