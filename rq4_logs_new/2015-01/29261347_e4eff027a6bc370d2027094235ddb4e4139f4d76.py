import re, nltk


#Grammar to deal with incoming questions about the DataSet
#It was defined in the question
grammar2 = nltk.parse_cfg("""
	D -> Player V Preposition Question "?" | Bat Question "?" | Player Preposition Question "?"
	V -> Bat | Bowl | Out | Over
	Out -> "was" "out" | "was" "out" Over
	Bowl -> "bowled" Number BowlType Over | "bowled" BowlType Over | "bowled" BowlType | "bowled" Number BowlType 
	Bat -> "hit" Number Runs Over | "hit" Runs | "hit" Runs Over | "hit" Number Runs | Number Runs Over Preposition | Runs Over Preposition
	Runs -> "ones" | "twos" | "threes" | "fours" | "sixes" | "one" | "two" | "three" | "four" | "six" | "no" "runs"
	Number -> "1" | "2" | "3" | "4" | "5" | "6" | "maximum"
	Player -> "Dhawan"| "Dhoni"| "Aaron"| "Ashwin"| "Binny"| "Jadeja"| "Kohli"| "Kumar"| "Mishra"| "Shami"| "Rahane"| "Raina"| "Rayudu"| "Sharma"| "Sharma" | "McCullum"| "Anderson"| "Benett"| "Guptill"| "Henry"| "McClenaghan"| "McCullum"| "Mills"| "Neesham"| "Ronchi"| "Ryder"| "Southee"| "Taylor" | "Williamson"| "Milne"
	Over -> "in" "overs" | "in" "over" OverIndex | "in" "over"
	OverIndex -> "1"| "2"| "3"| "4"| "5"| "6"| "7"| "8"| "9"| "10"| "11"| "12"| "13"| "14"| "15"| "16"| "17"| "18"| "19"| "20"| "21"| "22"| "23"| "24"| "25"| "26"| "27"| "28"| "29"| "30"| "31"| "32"| "33"| "34"| "35"| "36"| "37"| "38"| "39"| "40"| "41"| "42"| "43"| "44"| "45"| "46"| "47"| "48"| "49"| "50"
	BowlType -> "wide" | "wides" | "no" "ball" | 	"no" "balls"
	Question ->	"which"  "ball"  |  "which"  "over" |  "dismissed" "by" "whom" | "hit" "by" "whom" | "which" "bowler(s)" | "which" "overs"
	Preposition -> "in" | "of" | "was" | "were" | "hit"
	""")




def findRuns(s):
	if s in ["sixes" , "six"]:
		return "SIX"
	if s in ["fours" , "four"]:
		return "FOUR"
	if s in ["ones" , "one"]:
		return "1 run"
	if s in ["twos" , "two"]:
		return "2 runs"
	if s in ["threes" , "three"]:
		return "3 runs"
	if s in ["no" , "no run", "no runs"]:
		return "no run"



def findValue(token,tree):
	"""To find value of token values that come from Questions"""
	result = ""
	for i in tree :
		tmp = str(i)
		tmp = tmp.replace('(','')
		tmp = tmp.replace(')','')
		tmp = tmp.split()
		for j in xrange(len(tmp)):
			if tmp[j].find(token) != -1:
				if j+1 < len(tmp):
					result = tmp[j+1]
					if token == "Runs":
						result = findRuns(tmp[j+1])
					elif token == "BowlType":
						result = result[:-1]
					if token in ["Over","Out","Question"]:
						result = result + "  "+tmp[j+2]
					return result

					

def parseFile(filename):
	""" DataSet Files to be parsed and fed into the memory in the form of 
	Python Dictionaries	"""
	ballWise = {} # Dictionary for a ball by ball commentary
	widesAndNoballs = {} #Dictionary for Wides and No-balls
	ballNumber = re.compile("\d+.\d+") # Regular Expression to parse Balls
	rgcx1 = re.compile("\d+.\d+pm")
	rgcx2 = re.compile("\d+.\d+ pm")
	fileBuffer = "" # Buffer to hold the file
	#opening file to parse
	with open(filename) as fp:
	    for line in fp:
	    	fileBuffer = fileBuffer + line
	lines = fileBuffer.split('\n')
	for i in xrange(len(lines)):
		if i < len(lines) and not rgcx1.match(lines[i]) and not rgcx2.match(lines[i]) and  ballNumber.match(lines[i]):
			if lines[i] in ballWise.keys():
				if lines[i] in widesAndNoballs.keys():
					widesAndNoballs[str(float(lines[i])+0.1)] = ballWise[lines[i]]
				else:
					widesAndNoballs[lines[i]] = ballWise[lines[i]]
			ballWise[lines[i]] = lines[i+3]
			lines.remove(lines[i+3])
			i+=3
			fileBuffer.replace(lines[i+3],"")
	#print ballWise
	return ballWise,widesAndNoballs


	
	

def find_answer(filename,tokenValues):
	"""This function does most of the work where it finds the results 
	from the Dataset Dictionaries using the parsed tokens"""
	
	ballWise = {} # Dictionary for a ball by ball commentary
	widesAndNoballs = {} #Dictionary for Wides and No-balls
	ballWise, widesAndNoballs = parseFile(filename)			
	resultBalls = {}
	resultAnswer =None
	question = tokenValues['Question'].split()[1]
	

	if (tokenValues['V'] == 'Bat' or tokenValues['V'] == 'Out') and tokenValues['Player']!= None:
		if question != "dismissed":
			for i in ballWise.keys():
				if ballWise[i].find(tokenValues['Player']) != -1:
					resultBalls[i]=ballWise[i]
			for i in resultBalls.keys():
				if tokenValues['Runs'] != None and resultBalls[i].find(tokenValues['Runs']) == -1:
					del resultBalls[i]
			if tokenValues['OverIndex'] != None:
				for ball in resultBalls.keys():
					if ball[:ball.find('.')] != tokenValues['OverIndex']:
						del resultBalls[ball]
			
			if tokenValues['Number'] != None:
				runsOverWise = {}
				for ball in resultBalls.keys():
					if int(float(ball)) not in runsOverWise.keys():
						runsOverWise[int(float(ball))] = 1
					else:
						runsOverWise[int(float(ball))] += 1					
			
			if tokenValues['Out'] != None:
				for ball in resultBalls.keys():
					if resultBalls[ball].find("OUT,") == -1:
						del resultBalls[ball]
			overWiseRuns = {}
			if tokenValues['Number'] != None:
				for ball in resultBalls.keys():
					over= int(float(ball))
					if over in overWiseRuns.keys():
						overWiseRuns[over] += 1
					else :
						overWiseRuns[over] = 1
			
			#for i in resultBalls.keys():
			#	print i + ": " + resultBalls[i]
			if question == "bowlers":
				resultAnswer = []
				for bowl in resultBalls.keys():
					if resultBalls[bowl].split()[0] == "NL":
						tmp=resultBalls[bowl].split()[0]+" "+resultBalls[bowl].split()[1]
					else:
						tmp = resultBalls[bowl].split()[0]
					if tmp not in resultAnswer:
						resultAnswer.append(tmp)
				
			elif question == "ball":
				for bowl in resultBalls.keys():
					tmp = float(bowl)
					resultAnswer = int(tmp)* 6
					resultAnswer = str(int(float(bowl) * 10 ) %10) + "th ball"
				
			elif (question == "over" or question == "overs") and tokenValues['Number'] != None :
				if tokenValues['Number'] == 'maximum':
					resultOver = [0,0]				
					for i in overWiseRuns.keys():
						if overWiseRuns[i] > resultOver[1]:
							resultOver[0] = i
							resultOver[1] = overWiseRuns[i]
						elif overWiseRuns[i] == resultOver[1]:
							resultOver.append(i)
							resultOver.append(overWiseRuns[i])
				else:			
					resultOver = []
					for i in overWiseRuns.keys():
						if overWiseRuns[i] == int(tokenValues['Number']):
							if i not in resultOver:
								resultOver.append(i)
				resultAnswer = resultOver
					
				
			elif question == "over":
				print resultBalls
				resultAnswer=[]
				for i in resultBalls.keys():
					if int(float(i)) not in resultAnswer:
						resultAnswer.append(int(float(i)))
				
			elif question == "overs":
				resultAnswer = []
				for bowl in resultBalls.keys():
					tmp = int(float(bowl))
					if tmp not in resultAnswer:
						resultAnswer.append(tmp)
				
			if resultAnswer == [] or resultAnswer == None  or resultAnswer == {}:
				print "Doesn't exist"
			else:
				print resultAnswer
			

	elif tokenValues['V'] == 'Bowl' and tokenValues['Player']!= None:
		overWiseExtras = {}
		if tokenValues['Player'] != None :
			for i in widesAndNoballs.keys():
				if widesAndNoballs[i].find(tokenValues['Player']) != -1:
					resultBalls[i] = widesAndNoballs[i]
		
		if tokenValues['OverIndex'] != None :
			for ball in resultBalls.keys():
				if ball[:ball.find('.')] != tokenValues['OverIndex']:
					del resultBalls[ball]
		
		if tokenValues['BowlType'] != None:
			for ball in resultBalls.keys():
				if resultBalls[ball].find(tokenValues['BowlType']) == -1:
					del resultBalls[ball]
		
		if tokenValues['Number'] != None:
			for ball in resultBalls.keys():
				over= int(float(ball))
				if over in overWiseExtras.keys():
					overWiseExtras[over] += 1
				else :
					overWiseExtras[over] = 1
		print overWiseExtras
		#Dealing With Question tags Now
		print question
		if question == "over" or question == "overs":
			if tokenValues['Number'] == 'maximum':
				print "In Over"
				resultOver = [0,0]
				for i in overWiseExtras.keys():
					if overWiseExtras[i] > resultOver[1]:
						resultOver[0] = i
						resultOver[1] = overWiseExtras[i]
					elif overWiseExtras[i] == resultOver[1]:
						resultOver.append(i)
						resultOver.append(overWiseExtras[i])
			elif tokenValues['Number'] != None :
				resultOver = []
				for i in overWiseExtras.keys():
					if overWiseExtras[i] == int(tokenValues['Number']):
						resultOver.append(i)
			print resultOver



def findMatch(s):
	s=s.split()
	for i in s:
		if i == "first":
			return 1
		elif i == "second":
			return 2
		elif i == "third":
			return 3
		elif i == "fourth":
			return 4
		elif i == "fifth":
			return 5
	return None

	

def findFileName(match,player,verb):
	"""Function to find the innings file using the match number,players and verb
	e.g. for match 1, If Indian players are bowling then it's New Zealand Innings"""
	indPlayers = ["Dhawan", "Dhoni", "Aaron", "Ashwin", "Binny", "Jadeja", "Kohli", "Kumar", "Mishra", "Shami", "Rahane", "Raina", "Rayudu", "Sharma", "Sharma"]
	nzPlayers = ["McCullum", "Anderson", "Benett", "Guptill", "Henry", "McClenaghan", "McCullum", "Mills", "Neesham", "Ronchi", "Ryder", "Southee", "Taylor", "Williamson", "Milne"]
	if player in indPlayers and verb in ["Bat","Out"]:
		return  "odi"+str(match)+"_ininning.txt"
	if player in nzPlayers and verb in ["Bat","Out"]:
		return   "odi"+str(match)+"_nzinning.txt"
	if player in indPlayers and verb == "Bowl":
		return  "odi"+str(match)+"_nzinning.txt"
	if player in nzPlayers and verb == "Bowl":
		return "odi"+str(match)+"_ininning.txt"

def find_over_based_answers(match,tokenValues):
	"""This is to deal with Ball/Over based answers"""
	fileInd = "./dataset/odi"+str(match)+"_ininning.txt"
	fileNz = "./dataset/odi"+str(match)+"_nzinning.txt"
	ballWiseIn, widesAndNoballsIn = {} , {}
	ballWiseNz, widesAndNoballsNz = {} , {}
	ballWiseIn, widesAndNoballsIn = parseFile(fileInd)
	ballWiseNz, widesAndNoballsNz = parseFile(fileNz)
	InResult, NzResult, InWides, NzWides  = {}, {}, {}, {}
	if tokenValues['OverIndex'] != None :
		for ball in ballWiseIn.keys():
			if int(float(ball)) == int(tokenValues['OverIndex']):
				InResult[ball] = ballWiseIn[ball]
		for ball in ballWiseNz.keys():
			if int(float(ball)) == int(tokenValues['OverIndex']):
				NzResult[ball] = ballWiseNz[ball]
		for ball in widesAndNoballsIn.keys():
			if int(float(ball)) == int(tokenValues['OverIndex']):
				InWides[ball] = widesAndNoballsIn[ball]
		for ball in widesAndNoballsNz.keys():
			if int(float(ball)) == int(tokenValues['OverIndex']):
				NzWides[ball] = widesAndNoballsNz[ball]
	
	if tokenValues['Runs'] != None:
		for ball in InResult.keys():
			if InResult[ball].find(tokenValues['Runs']) == -1:
				del InResult[ball]
		for ball in NzResult.keys():
			if NzResult[ball].find(tokenValues['Runs']) == -1:
				del NzResult[ball]
	playerWiseIn, playerWiseNz = {}, {}

	for ball in InResult.keys():
		player = InResult[ball].split()[2]
		if player == "to":
			player = InResult[ball].split()[3]
		if player in playerWiseIn.keys():
			playerWiseIn[player] += 1
		else:
			playerWiseIn[player] = 1
	for ball in NzResult.keys():
		player = NzResult[ball].split()[2]
		if player == "to":
			player = NzResult[ball].split()[3]
		if player in playerWiseNz.keys():
			playerWiseNz[player] += 1
		else:
			playerWiseNz[player] = 1
	print playerWiseNz,playerWiseIn




def add_to_dict(dictionary, fname):
	f = open(fname, 'r')
	for line in f:
		temp = line[:-1]
		temp = temp.split(',')
		a = temp[0]
		b = temp[1:]
		if a not in dictionary:
			dictionary[a] = b

def findDismissed(fname,player):
	playerData = {}
	f = open(fname, 'r')
	for line in f:
		temp = line[:-1]
		temp = temp.split(',')
		a = temp[0]
		b = temp[1:]
		if a not in playerData:
			playerData[a] = b

	for i in playerData.keys():
		if player in i.split():
			print playerData[i][0]


def main():
	tokens = ['Player','V','Runs','Number','Over', 'OverIndex' , 'BowlType','Question', 'Out' ]
	rd_parser = nltk.RecursiveDescentParser(grammar2)
	sent = raw_input()
	match = sent[:sent.find(',')]
	sent = sent[sent.find(',')+1 :]
	
	sent=sent.split()
	
	tokenValues = {}
	tree = rd_parser.nbest_parse(sent)
	
	for token in tokens:
		tokenValues[token] = findValue(token,tree)
	
	if tokenValues['Player'] != None:
		if tokenValues['Question'] == "dismissed  by":
			filename = findFileName(findMatch(match),tokenValues['Player'],'Bat')
			filename = "./dataset/scoreBoard/"+filename
			findDismissed(filename,tokenValues['Player'])
		else :
			filename = findFileName(findMatch(match),tokenValues['Player'],tokenValues['V'])
			filename = "./dataset/"+filename
			find_answer(filename,tokenValues) 
	else:
		find_over_based_answers(findMatch(match),tokenValues)


if __name__ == "__main__":
	main()