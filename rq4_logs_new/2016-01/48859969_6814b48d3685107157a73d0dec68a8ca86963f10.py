#Cowboy shootout simulator
import random as rd
#3 cowboys in a duel to the death. You are the fastest (but worst) shot, the
#dreaded Cowboy A.  The next fastest (and slightly better) shot is Cowboy B.
#Slowest (and best) shot is Cowboy C.  You fire, then B (if alive), then C (if
#alive), then you again (if alive). What is the best strategy?

def CowboySim(nDuels = 1, accuracies = [0.25, 1./3, 0.5], targets = ['C','C','A']):	
	results = {}
	target2Idx = lambda t: 0 if t == 'A' else 1 if t == 'B' else 2 if t == 'C' else -1
	isHit = lambda idx: rd.random()<accuracies[idx]
	nextTarget = lambda n,t: 'A' if ((n != 0) and ((t=='B') or (t == 'C'))) else 'B' if ((n != 1) and ((t=='A') or (t == 'C'))) else 'C' 
	cowboyVictories=[0,0,0]
	for duelNum in range(nDuels):
		results[duelNum] = {}
		cowboyHealth = [1,1,1]
		duelOngoing = True
		results[duelNum]['Shots']=[]
		while duelOngoing:							
			for cowboyIdx in range(3):
				if targets[cowboyIdx] in 'ABC':
					pass
				else:
					continue
				if cowboyHealth[cowboyIdx] > 0:
					currentShotStatus = isHit(cowboyIdx) #generate new shot
					results[duelNum]['Shots'].append(currentShotStatus)					
					if currentShotStatus == True:
						print "CowboyIndex " + str(cowboyIdx) + " shot Cowboy " + targets[cowboyIdx]
						cowboyHealth[target2Idx(targets[cowboyIdx])] = 0
						#set next target for all other cowboys
						otherCowboyIdxs=range(3)
						temp = otherCowboyIdxs.pop(target2Idx(targets[cowboyIdx]))
						print(otherCowboyIdxs)
						for otherCowboyIdx in otherCowboyIdxs:
							targets[otherCowboyIdx] = nextTarget(otherCowboyIdx,targets[cowboyIdx])
						targets[cowboyIdx] = nextTarget(cowboyIdx, targets[cowboyIdx])

			if sum(cowboyHealth) == 1:
				duelOngoing = False
			
		#tabulate results for the current duel
		results[duelNum]['Winner'] = 'A' if cowboyHealth == [1,0,0] else 'B' if cowboyHealth == [0,1,0] else 'C'		
		cowboyVictories[cowboyIdx] += 1

	return results, cowboyVictories