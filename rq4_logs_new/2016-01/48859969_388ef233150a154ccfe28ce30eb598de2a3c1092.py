#Birthday Paradox Simulator
from random import *
from math import *

def BDSim(N=100):	
	ppl2BD = []
	for iterval in range(N):
		ppl2BD.append(0)
		BDList = []
		matchFound = False
		while not matchFound:
			#add a Bday
			BDList.append(floor(365*random()))
			ppl2BD[iterval] += 1			
			#Check for a match with new day
			if BDList != uniquify(BDList):
				matchFound = True
	return ppl2BD


#uniquify taken from http://www.peterbe.com/plog/uniqifiers-benchmark
def uniquify(seq, idfun=None): 
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result