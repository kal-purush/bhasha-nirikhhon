import json
import urllib2
import sys

def getPercentage(threshold, points):
	newlist = [x for x in points if x >= threshold]

	var = float(len(newlist))
	var2 = float(len(points)) 

	return ((var/var2)*100)

def getAvgThreshold(percentage, points):
	index = int((len(points)*(percentage/100)))
	return points[index]

def avg(a):
	return sum(a)/len(a)

def getData():
	#read commandline args
	startTime = sys.argv[1] # in unix time format

	# Acquire data from REST API
	url = 'http://178.62.40.4/api/readings-unixtime?start='+str(startTime)
	resp = urllib2.urlopen(url)
	return json.loads(resp.read())

def createAnomalyScoreList(data):
	# create list of anomaly scores for all points in data
	anomalyscores = []
	for x in data["readings"]:
		if "anomalyScore" in x:
			anomalyscores.append(x["anomalyScore"])

	sorted(anomalyscores)
	return anomalyscores

def createFeedbackList(data):
	#create list of tuples, containing anomaly scores with feedback in the form (score,classification,feedback)
	feedBackPoints = [] 
	for x in data["readings"]:
		if "anomalyScore" in x and "classification" in x:
			if "feedback" in x:
				feedBackPoints.append((x["anomalyScore"], x["classification"], x["feedback"]))

	return feedBackPoints

def splitFeedbackList(feedBackPoints):
	# Split the points with feedback into seperate categories, based on classification and user's feed back. eg: Classifier: Amber, Feedback: red
	GR = []
	GA = []
	AR = []
	AG = []
	RA = []
	RG = []

	for x in feedBackPoints:
		if x[1] == "green":
			if x[2] == "red":
				GR.append(x)
				continue
			if x[2] == "amber":
				GA.append(x)
				continue
		if x[1] == "amber":
			if x[2] == "red":
				AR.append(x)
				continue
			if x[2] == "green":
				AG.append(x)
				continue

		if x[1] == "red":
			if x[2] == "amber":
				RA.append(x)
				continue
			if x[2] == "green":
				RG.append(x)
				continue

	return (GR,GA,AR,AG,RA,RG)


def formulaUP(score, threshold):
	return (BASE_INCREASE_MULTIPLIER*((FORMULA_UP_CONST/score) - threshold))

def formulaDOWN(score, threshold):
	return	(BASE_DECREASE_MULTIPLIER/((FORMULA_DOWN_CONST/score) - threshold))  

def calculateNewThresholds(feedbackList, previousRedThreshold, previousAmberThreshold):
	fsplits = splitFeedbackList(feedbackList)
	avgFSplits = (avg(fsplits[0]),avg(fsplits[1]),avg(fsplits[2]),avg(fsplits[3]),avg(fsplits[4]),avg(fsplits[5]))
	lenFSplits = (len(fsplits[0]),len(fsplits[1]),len(fsplits[2]),len(fsplits[3]),len(fsplits[4]),len(fsplits[5]))
	redmultipliers = []
	ambermultipliers = []

	#scenario 1
	redmultipliers.append(formulaDOWN(avgFSplits[0],previousRedThreshold))
	ambermultipliers.append(redmultipliers[0])

	#scenario 2
	ambermultipliers.append(formulaDOWN(avgFSplits[1],previousAmberThreshold))

	#scenario 3
	redmultipliers.append(formulaDOWN(avgFSplits[2],previousRedThreshold))
	
	#scenario 4
	ambermultipliers.append(formulaUP(avgFSplits[3],previousAmberThreshold))

	#scenario 5
	redmultipliers.append(formulaUP(avgFSplits[4],previousRedThreshold))
	
	#secnario 6
	redmultipliers.append(formulaUP(avgFSplits[5],previousRedThreshold))
	ambermultipliers.append(redmultipliers[3])

	newRedThreshold = 0
	newAmberThreshold = 0

	newRedThreshold+=lenFSplits[0]*redmultipliers[0]
	newRedThreshold+=lenFSplits[2]*redmultipliers[1]
	newRedThreshold+=lenFSplits[4]*redmultipliers[2]
	newRedThreshold+=lenFSplits[5]*redmultipliers[3]

	newAmberThreshold+=lenFSplits[0]*ambermultipliers[0]
	newAmberThreshold+=lenFSplits[1]*ambermultipliers[1]
	newAmberThreshold+=lenFSplits[3]*ambermultipliers[2]
	newAmberThreshold+=lenFSplits[5]*ambermultipliers[3]

	newRedThreshold/=(sum(lenFSplits)-(lenFSplits[1]+lenFSplits[3]))
	newAmberThreshold/=(sum(lenFSplits)-(lenFSplits[2]+lenFSplits[4]))

	newRedThreshold = (newRedThreshold+previousRedThreshold)/2
	newAmberThreshold = (newAmberThreshold+previousAmberThreshold)/2

	return (newRedThreshold,newAmberThreshold)


#								Execution begins here
########################################################################################
BASE_INCREASE_MULTIPLIER = 1.2
BASE_DECREASE_MUTIPLIER = 0.8
FORMULA_UP_CONST = 1.0
FORMULA_DOWN_CONST = 1.0



previousRedPercentage = float(sys.argv[2])
previousAmberPercentage = float(sys.argv[3])
previousRedThreshold = float(sys.argv[4])
previousAmberThreshold = float(sys.argv[5])


data = getData()
anomalyscores = createAnomalyScoreList(data)


feedbackList = createFeedbackList(data)

if(len(feedbackList)==0):
	currentRedPercentage = getPercentage(previousRedThreshold, anomalyscores)
	currentAmberPercentage = getPercentage(previousAmberThreshold, anomalyscores)
	newRedThreshold = getAvgThreshold(((currentRedPercentage+previousRedPercentage)/2), anomalyscores)
	newAmberThreshold = getAvgThreshold(((currentAmberPercentage+previousAmberPercentage)/2), anomalyscores)
	#add some code to pass these calculated values to system
	#print "amber is "+ str(newAmberThreshold) + " red is "+str(newRedThreshold)
	#print "amber percentage is "+ str(currentAmberPercentage) + " red percentage is " +str(currentRedPercentage)
else:
	newThresholds = calculateNewThresholds(feedbackList)
	#print "amber is "+ str(newThresholds[1]) + " red is "+ str(newThreholds[0])


