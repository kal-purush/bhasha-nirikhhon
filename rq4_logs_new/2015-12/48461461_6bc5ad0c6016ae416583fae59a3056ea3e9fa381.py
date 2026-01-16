from pickle import dump,load
from datetime import date
"""
+==============================================================================================+
|            Legend				|							Time of Snow					   |
|-------------------------------|-----------------+--------------+-------------+---------------+
|            					| BD = Before Day | MO = Morning | MD = Midday | LD = Late Day |
|-------------------------------|-----------------+--------------+-------------+---------------+
|OP = Open     					|		  3		  |		  1		 |		1	   |		2	   |
|-------------------------------|--------------------------------------------------------------+
|DY = 2 Hour Delay				|		  2		  |		  3		 |		0	   |		0	   |
|-------------------------------|--------------------------------------------------------------+
|ED = 2 Hour Early Dismissal	|		  0		  |		  0		 |		2	   |		3	   |
|-------------------------------|--------------------------------------------------------------+
|CL = Closed					|		  1		  |		  2		 |		3	   |		1	   |
|-------------------------------|--------------------------------------------------------------+
|ND = Not Decided				|		  -1	  |		  -1	 |		-1	   |		-1	   |
+==============================================================================================+
peopleScale = {number:[percent,choice,over,equal,under,streak]}
Percents = {choice:percent}
data = {Date,Voted Result,Result}
"""
Legend = {"BD":{"OP":3,"DY":2,"ED":0,"CL":1},"MO":{"OP":1,"DY":3,"ED":0,"CL":2},"MD":{"OP":1,"DY":0,"ED":2,"CL":3},"LD":{"OP":2,"DY":0,"ED":3,"CL":1}}
def createNew():
	numPeople = int(input("Number of People:\t"))
	percent = 100/numPeople
	newFile = open('Snow.txt','wb')
	peopleScale = {}
	for x in range(numPeople):
		peopleScale[x] = [percent,'OP',0,0,0,0]
	data = {}
	dumpdata = [peopleScale,data]
	dump(dumpdata,newFile)
def Question():
	peopleScale,data = load(open("Snow.txt",'rb'))
	today = str(date.today())
	today = today[2:4]+today[5:7]+today[8:10]
	data[today] = ["",'','']
	Snowtime = data[today][0] = input("Snowtime:\t\t")
	Percents = {'OP':0,'DY':0,'ED':0,'CL':0,'NA':0}
	for numPeople in peopleScale:
		peopleScale[numPeople][1] = input("Person "+str(numPeople+1)+" what do you think?")
		Percents[peopleScale[numPeople][1]] += peopleScale[numPeople][0] + peopleScale[numPeople][5]
		Percents = overAndUnderAdjustment(peopleScale[numPeople],Percents,Snowtime)
	data[today][1] = best = findMax(Percents)
	print(best)
	newFile = open('Snow.txt','wb')
	dumpdata = [peopleScale,data]
	dump(dumpdata,newFile)
def findMax(Percents):
	maximum = 0,''
	for x in Percents:
		if Percents[x]>maximum[0] and x != 'NA':
			maximum = Percents[x],x
	return maximum[1]
def improve():
	date = input("Date(YYMMDD):\t")
	correct = input("Result of Day:\t")
	peopleScale,data = load(open("Snow.txt",'rb'))
	data[date][2] = correct
	Snowtime = data[date][0]
	Equal = []
	NotEqual = []
	num = Legend[Snowtime][correct]
	for numPeople in peopleScale:
		if num != Legend[Snowtime][peopleScale[numPeople][1]]:
			NotEqual.append(numPeople)
			peopleScale[numPeople][5] = 0
		else:
			peopleScale[numPeople][3] += 1
			peopleScale[numPeople][5] += 1
			Equal.append(numPeople)
		if num > Legend[Snowtime][peopleScale[numPeople][1]]:
			peopleScale[numPeople][4] += 1
		elif num < Legend[Snowtime][peopleScale[numPeople][1]]:
			peopleScale[numPeople][2] += 1
	peopleScale = adjustPercents(peopleScale,Equal,NotEqual)
	newFile = open('Snow.txt','wb')
	dumpdata = [peopleScale,data]
	dump(dumpdata,newFile)
def adjustPercents(peopleScale,Equal,NotEqual):
	percent = 0
	for x in NotEqual:
		percent += peopleScale[x][0]/10								#Constant
		peopleScale[x][0] = peopleScale[x][0]-peopleScale[x][0]/10 	#Constant
	percent = percent/len(Equal)
	for x in Equal:
		peopleScale[x][0]+= percent
	return peopleScale
def overAndUnderAdjustment(person,Percents,Snowtime):
	adjustment = Legend[Snowtime]
	for x in Percents:
		if x == 'NA':
			continue
		#print(adjustment,adjustment)
		if (adjustment[x]>adjustment[person[1]]) and person[2]:
			Percents[x] +=person[2]
		if (adjustment[x]<adjustment[person[1]]) and person[4]:
			Percents[x] +=person[4]
	return Percents
def printData():
	peopleScale,data = load(open("Snow.txt",'rb'))
	print(peopleScale,data)

#createNew()
Question()
printData()
improve()
printData()