import random
import sys

class adaptableClass:

	chance = 0

	def __init__(self):
		self.chance = random.randint(1, 10)/10

	def increase(self):
		if self.chance != 1.0:
			self.chance = (self.chance * 10 + 1) / 10
	def decrease(self):
		if self.chance != 0.1:
			self.chance = (self.chance * 10 - 1) / 10

def initgen(popsize):
	pop = []
	for person in range(popsize):
		pop.append(adaptableClass())
	return pop

def whogoes(pop):

	golist = []
	nolist = []

	for p in range(len(pop)):
		rand = random.randint(0, 10)/10
		if pop[p].chance >= rand:
			golist.append(p)
		else:
			nolist.append(p)

	return golist, nolist

def dicter(go, no, pop):
	godict = {}
	nodict = {}

	for goer in go:
		current = str(pop[goer].chance)
		if current in godict:
			godict[current] += 1
		else:
			godict[current] = 1

	for noer in no:
		current = str(pop[noer].chance)
		if current in nodict:
			nodict[current] += 1
		else:
			nodict[current] = 1

	return godict, nodict

def humanreadable(godict, nodict, popsize, iterations, go, point):

	if len(go) < point:
		status = "Going to the El Farol"
	else:
		status = "Staying at home"

	sort = sorted(set(list(godict.keys()) + list(nodict.keys())))
	print("Iteration ", iterations, ":", sep = "")
	print(status, "was the better choice.")
	print("Chance", "	", "Go", "	", "No go", "	", "Total", sep = "")
	for s in sort:
		gofreq = float(godict.get(s, 0))/popsize * 100
		nofreq = float(nodict.get(s, 0))/popsize * 100
		allfreq = (float(godict.get (s, 0)) + float(nodict.get (s, 0)))/popsize * 100
		print(s, sep = "", end = "	")
		print("%.2f" % gofreq, "%", sep = "", end = "	")
		print("%.2f" % nofreq, "%", sep = "", end = "	")
		print("%.2f" % allfreq, "%", sep = "", end = "	")
		print(int(allfreq)* "*", sep = "")
	print()

def evaler(go, pop, popsize, point):

	if len(go) < point:
		success = 1
		for g in go:
			pop[g].increase()
	else:
		success = 0
		for g in go:
			pop[g].decrease()
	
	return success

def main():

	goal = int(sys.argv[1])
	popsize = int(sys.argv[2])

	pop = initgen(popsize)

	success = 0
	
	for i in range(goal):
		point = popsize * 0.6
		go, no = whogoes(pop)

		godict, nodict = dicter(go, no, pop)
		humanreadable(godict, nodict, popsize, i, go, point)

		success += evaler(go, pop, popsize, point)
	print("El Farol was the better choice in ", success / goal *100, "% of the cases",sep = "")

if __name__ == '__main__':
	main()