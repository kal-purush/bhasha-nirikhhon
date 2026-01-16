
from collegeSwimming001.models import Swimmer,Time
import math

baseTimes = [18.38,40.56,90.74,247.30,513.91,859.20,43.85,96.29,49.79,108.12,43.96,98.81,98.88,213.43]

def convertTime(time):
	if time==None :
		return 100000
	else:
		timeClean= time.time.strip(' ')
		if ':' in timeClean: 
			if timeClean.index(':')==1:
				return float(timeClean[0])*60 + float(timeClean[2:4]) + 0.01*float(timeClean[5:7])
			if timeClean.index(':')==2:
				return float(timeClean[0:2])*60 + float(timeClean[3:5]) + 0.01*float(timeClean[6:7])
		else:
			return float(timeClean[0:timeClean.index('.')]) + 0.01*float(timeClean[timeClean.index('.')+1:5])


#takes scores in each event and returns a score for Sprint, Distance, Back, Breast,Fly
def calculateScores(swimmerID):
	swimmerTimes = Time.objects.filter(swimmer=swimmerID)
	
	#get all times 
	fiftyFreeTime = convertTime(swimmerTimes.filter(event__stroke='1').filter(event__distance=50).order_by('time').first())
	oneFreeTime = convertTime(swimmerTimes.filter(event__stroke='1').filter(event__distance=100).order_by('time').first())
	twoFreeTime= convertTime(swimmerTimes.filter(event__stroke='1').filter(event__distance=200).order_by('time').first())
	fiveFreeTime = convertTime(swimmerTimes.filter(event__stroke='1').filter(event__distance=500).order_by('time').first())
	tenFreeTime = convertTime(swimmerTimes.filter(event__stroke='1').filter(event__distance=1000).order_by('time').first())
	mileTime = convertTime(swimmerTimes.filter(event__stroke='1').filter(event__distance=1650).order_by('time').first())
	oneBackTime = convertTime(swimmerTimes.filter(event__stroke='2').filter(event__distance=100).order_by('time').first())
	twoBackTime= convertTime(swimmerTimes.filter(event__stroke='2').filter(event__distance=200).order_by('time').first())
	oneBreastTime = convertTime(swimmerTimes.filter(event__stroke='3').filter(event__distance=100).order_by('time').first())
	twoBreastTime = convertTime(swimmerTimes.filter(event__stroke='3').filter(event__distance=200).order_by('time').first())
	oneFlyTime= convertTime(swimmerTimes.filter(event__stroke='4').filter(event__distance=100).order_by('time').first())
	twoFlyTime= convertTime(swimmerTimes.filter(event__stroke='4').filter(event__distance=200).order_by('time').first())
	twoIMTime= convertTime(swimmerTimes.filter(event__stroke='5').filter(event__distance=200).order_by('time').first())
	fourIMTime= convertTime(swimmerTimes.filter(event__stroke='5').filter(event__distance=400).order_by('time').first())
	
	#calculate scores
	sprintScore = (math.pow((baseTimes[0]/fiftyFreeTime),3)*100 + math.pow((baseTimes[1]/oneFreeTime),3)*100)/2
	distanceScore = (math.pow((baseTimes[3]/fiveFreeTime),3)*100 + math.pow((baseTimes[4]/tenFreeTime),3)*100 + math.pow((baseTimes[5]/mileTime),3)*100)/3
	backScore = (math.pow((baseTimes[6]/oneBackTime),3)*100 + math.pow((baseTimes[7]/twoBackTime),3)*100)/2
	breastScore = (math.pow((baseTimes[8]/oneBreastTime),3)*100 + math.pow((baseTimes[9]/twoBreastTime),3)*100)/2
	flyScore = (math.pow((baseTimes[10]/oneFlyTime),3)*100 + math.pow((baseTimes[11]/twoFlyTime),3)*100)/2
	IMScore = (math.pow((baseTimes[12]/twoIMTime),3)*100 + math.pow((baseTimes[13]/fourIMTime),3)*100)/2
	
	
	return [sprintScore,backScore,breastScore,flyScore,IMScore,distanceScore]
	
	
	
	
	