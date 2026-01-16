import pdb
import itertools
import copy
import operator

#file opening
filename = raw_input("Enter the inputfile name: ")
f = open(filename)

#data cleaning
a = [x for x in f.readlines() if x != '\r\n']
a = [w.replace(' | ', ',') for w in a]
a = [w.replace("\r\n", '') for w in a]

#data splitting 
peopleList = []
offersList = []
relationshipsList = []

for x in range(1 , a.index('offers')):
	peopleList.append(a[x])

for x in range(a.index('offers')+1 , a.index('relationships')):
	offersList.append(a[x])

for x in range(a.index('relationships')+1 , len(a)):
	relationshipsList.append(a[x])

#put data into lists of dictionaries
people = []
offers = []
relationships = []

for person in peopleList:
	temp = {}
	tempList = person.split(',')
	temp['name'] = tempList[0]
	temp['type'] = tempList[1]
	people.append(temp)

for offer in offersList:
	temp = {}
	tempList = offer.split(',')
	temp['name'] = tempList[0]
	temp['company'] = tempList[1]
	temp['type'] = tempList[2]
	temp['location'] = tempList[3]
	temp['utility'] = 0
	offers.append(temp)

for relationship in relationshipsList:
	temp = {}
	tempList = relationship.split(',')
	temp['p1'] = tempList[0]
	temp['p2'] = tempList[1]
	temp['relationship'] = tempList[2]
	relationships.append(temp)

#data mockup, for people, offers, relationships, they should be like these:
#
#people = [{'name':'Amy','type':'Academic'},
#          {'name':'Bob','type':'Entrepreneur'},
#          {'name':'Charlie','type':'Money Grubber'}]
#
#offers = [{'name':'Amy','company':'MacroHard','type':'Big Software Firm','location':'Seattle','utility':0},
#		  {'name':'Amy','company':'Stanguard College','type':'Grad School','location':'San Francisco','utility':0},
#		  {'name':'Amy','company':'Dartboard Modeling','type':'Hedge Fund','location':'NYC','utility':0},
#		  {'name':'Bob','company':'Bigup-Side','type':'Startup','location':'NYC','utility':0},
#		  {'name':'Bob','company':'Questionable Tactics','type':'Hedge Fund','location':'San Francisco','utility':0},
#		  {'name':'Charlie','company':'Cash-Money Inc.','type':'Investment Bank','location':'NYC','utility':0},
#		  {'name':'Charlie','company':'Arbitrack','type':'Hedge Fund','location':'San Francisco','utility':0}]
#
#relationships = [{'p1':'Bob','p2':'Amy','relationship':'Dating'},
#				 {'p1':'Bob','p2':'Charlie','relationship':'Mortal Enemies'}]
#end of data processing


#initialize given constraints
benefits = [{'type':'Big Software Firm','pay':6,'hours':6,'impact':2,'learn':8},
			{'type':'Hedge Fund','pay':8,'hours':8,'impact':4,'learn':6},
			{'type':'Investment Bank','pay':10,'hours':10,'impact':3,'learn':4},
			{'type':'Startup','pay':4,'hours':8,'impact':10,'learn':8},
			{'type':'Grad School','pay':1,'hours':4,'impact':3,'learn':10}]


personalities = [{'type':'Money Grubber','pay':10,'hours':-1,'impact':4,'learn':2},
				 {'type':'Entrepreneur','pay':4,'hours':-2,'impact':10,'learn':8},
				 {'type':'Slacker','pay':1,'hours':-10,'impact':2,'learn':2},
				 {'type':'Academic','pay':2,'hours':-6,'impact':8,'learn':10}]


#this is a function which calculates the bonus between 2 given people
def relationshipBonus (name1, name2):
	for relationship in relationships:
		if (name1 == relationship['p1'] and name2 == relationship['p2']) or (name2 == relationship['p1'] and name1 == relationship['p2']):
			if relationship['relationship'] == 'Dating':
				return 50
			if relationship['relationship'] == 'Mortal Enemies':
				return -1000
			if relationship['relationship'] == 'Friends':
				return 20
			if relationship['relationship'] == 'Married':  
				return 1000
	return 0

#caculate the utility for each offer in the offers list
for person in people:
	for personality in personalities:
		if personality['type'] == person['type']:
			for offer in offers:
				if offer['name'] == person['name']:
					for benefit in benefits:
						if benefit['type'] == offer['type']:
							offer['utility'] = benefit['pay']*personality['pay']+benefit['hours']*personality['hours']+benefit['impact']*personality['impact']+benefit['learn']*personality['learn']

#create a list of all possible combinations of assignments 
#if Amy has 3 choices, Bob has 2, and Charlie has 2, then the number of all possible combinations is 3*2*2 = 12
personalOffers = []
for person in people:
	temp = []
	for offer in offers:
		if offer['name'] == person['name']:
			temp.append(offer)
	personalOffers.append(temp)
combinations = map(list,itertools.product( *personalOffers))

#for each combination, calculate the utility for each offer considering the geography
results = []
for combination in combinations:
	assignments = []
	total = 0
	for offer in combination:
		temp = {}
		bonus = 0
		for otherOffer in combination:
			if otherOffer['name'] != offer['name']:
				if otherOffer['location'] == offer['location']:
					bonus = relationshipBonus(otherOffer['name'],offer['name']) +bonus
		temp = {'name':offer['name'],'company':offer['company'],'utility':offer['utility']+bonus}
		assignments.append(temp)
		total =total +temp['utility']
	results.append({'assignments':assignments,'totalUtility':total})

#sort the results list by the total utility
results.sort(key=operator.itemgetter('totalUtility'))
optimal = results.pop()

print ("the optimal assignments are:")
for offer in optimal['assignments']:
	print (offer['name'] + ' --> ' + offer['company'])