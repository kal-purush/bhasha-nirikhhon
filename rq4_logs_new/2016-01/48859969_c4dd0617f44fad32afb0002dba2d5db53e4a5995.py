import math

class Population(object):
	"""A Population"""
	def __init__(self, numPeople, rate = 0.0,  detectRate = 0.0):
		#super(Population, self).__init__(self, numPeople)
		self.numPeople = numPeople
		self.rate = rate
		self.detectRate = detectRate

#Make a few populations
A = {}

startPops = [1e6,5e6,5e5]
startRates = [0.2,0.25,0.1]
startDetectRates = [0.8,0.1,0.4]

for popKeyIter in range(len(startPops)):
	# A[0] = Population(1e6,0.2)
	# A[1] = Population(5e6,0.25)
	# A[2] = Population(5e5,0.1)
	print popKeyIter
	A[popKeyIter] = Population(startPops[popKeyIter],startRates[popKeyIter],startDetectRates[popKeyIter])

print A
#initialized pops, begin sim

timeStep = 0
simulationTime = 100
detectedPop=[]
detectRates = []
timeDetectAdjustmentFactor = 0.1
for timeStep in range(simulationTime):
	#if timeStep > 0:
	totalPop = 0
	detectedPop.append([])
	detectRates.append([])
	for keyIter in A.keys():
		totalPop +=A[keyIter].numPeople
		detectedPop[timeStep].append(round(A[keyIter].detectRate*A[keyIter].rate * A[keyIter].numPeople))
		detectRates[timeStep].append(A[keyIter].detectRate)
	print detectedPop[timeStep]
	#make adjustment to Population.detectRate
	detectedFraction=[each/sum(detectedPop[timeStep]) for each in detectedPop[timeStep]]	
	observedIncidence =[detectedPop[timeStep][it]/startPops[it] for it in range(len(startPops))]
	normIncidence=[each/sum(observedIncidence) for each in observedIncidence]
	normDetect = [each/sum(detectRates[timeStep]) for each in detectRates[timeStep]]
	adjustments = [timeDetectAdjustmentFactor*(normIncidence[it]-normDetect[it]) for it in range(len(normDetect))]
	print "normIncidence at timeStep " + str(timeStep) + ": " + str(normIncidence)
	print "normDetect at timeStep " + str(timeStep) + ": " + str(normDetect)
	print "adjustments at timeStep " + str(timeStep) + ": " + str(adjustments)
	#detectionIncidence = [each/sum(startDetectRates) for each in startDetectRates]
	for keyIter in A.keys():
		A[keyIter].detectRate += adjustments[keyIter]
		if A[keyIter].detectRate > 1.0:
			A[keyIter].detectRate = 1.0
		elif  A[keyIter].detectRate < 0.0:
			A[keyIter].detectRate =0.0
		# if detectedFraction[keyIter] > A[keyIter].detectRate:
		# 	#raise detectionRate for given pop, as actual detected fraction exceeds detectRate
	 # 		A[keyIter].detectRate += timeDetectAdjustmentFactor*detectedFraction[keyIter]
 	# 	else:
 	# 		A[keyIter].detectRate -= timeDetectAdjustmentFactor*detectedFraction[keyIter]
 	newDetectRates = [A[it].detectRate for it in range(len(startPops))]
 	print "newDetectRates at timeStep " + str(timeStep) + ": " + str(newDetectRates)
#print final results
for keyIter in A.keys():
	print "Detected rate for population "+ str(keyIter) +" after " + str(simulationTime) + " time steps was " + str(A[keyIter].detectRate)