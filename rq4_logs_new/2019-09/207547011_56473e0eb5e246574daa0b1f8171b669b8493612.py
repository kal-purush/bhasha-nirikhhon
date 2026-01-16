import time
from itertools import permutations

alphet_to_num = {"A":0,"B":1,"C":2,"D":3,"E":4,"F":5,"G":6,"H":7,"I":8,"J":9,"K":10,"L":11,"M":12,"N":13,"O":14,"P":15,"Q":16,"R":17,"S":18,
                "T":19,"U":20,"V":21,"W":22,"X":23,"Y":24,"Z":25}
num_to_alphet = {0:"A",1:"B",2:"C",3:"D",4:"E",5:"F",6:"G",7:"H",8:"I",9:"J",10:"K",11:"L",12:"M",13:"N",14:"O",15:"P",16:"Q",17:"R",18:"S",
                19:"T",20:"U",21:"V",22:"W",23:"X",24:"Y",25:"Z"}

import random 
def randomPlugboard(): 
    plug = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25] #原始plugboard 
    alreadyUseList = [] #用來記錄哪些已經換過了 
    i = 1
    while i <= 6:
        firstNum = random.randint(0,25)#random 第1個數字(0~25)
        secondNum = random.randint(0,25)
        #secondNum = random.randint(0,25)#random 第2個數字(0~25)
        #if兩個數字其中一個已經換過了(存在於alreadyUseList)
        if firstNum in alreadyUseList:
            continue
        elif secondNum in alreadyUseList:
            continue
        elif firstNum == secondNum:
            continue
        else:
            firstNumIndex = plug.index(firstNum)
            secondNumIndex = plug.index(secondNum)
            plug[firstNumIndex] = secondNum
            plug[secondNumIndex] = firstNum
            alreadyUseList.append(firstNum)
            alreadyUseList.append(secondNum)
            i+=1
    return plug

def rotate(rotor):
    tmp_r = rotor.pop(0)
    rotor.append(tmp_r)
    return rotor
    
def initial(outerrotor,innerrotor , startposition):
    while(outerrotor[0] !=startposition):
        outerrotor = rotate(outerrotor)
        innerrotor = rotate(innerrotor)
    return outerrotor , innerrotor

def read_file(filename):
    global alphet_to_num
    f = open(filename, "r")
    lines = f.readlines()
    string = ""
    for line in lines:
        line.strip('\n')
        string = string+line
    lst = list(string)
    file = [alphet_to_num[char] for char in lst]
    return file

def enigma(plugboard, rotorIII, rotorII, rotorI, arrow, start, input_string):
    global alphet_to_num, num_to_alphet
    arrow[0] = rotorIII[arrow[0]]
    arrow[1] = rotorII[arrow[1]]
    arrow[2] = rotorI[arrow[2]]
    
    RIII_outside = read_file('outside.txt')
    RII_outside = read_file('outside.txt')
    RI_outside = read_file('outside.txt')
    reflector = read_file('reflector_web.txt')
    RIII_outside, rotorIII = initial(RIII_outside, rotorIII, start[0])
    RII_outside, rotorII = initial(RII_outside, rotorII, start[1])
    RI_outside, rotorI = initial(RI_outside, rotorI, start[2])
    input = [alphet_to_num[char] for char in input_string]
    output = list()
    for input_char in input:
        rotorIII = rotate(rotorIII)
        RIII_outside = rotate( RIII_outside)
        if (rotorII[1] == arrow[1] and rotorIII[0] != arrow[0]) or rotorIII[0] == arrow[0]:
            rotorII = rotate(rotorII)
            RII_outside = rotate(RII_outside)
        if rotorII[0] == arrow[1]:
            rotorI = rotate(rotorI)
            RI_outside = rotate(RI_outside)
        plug = plugboard[input_char]
           
        RIII = RIII_outside.index(rotorIII[plug])
           
        RII = RII_outside.index(rotorII[RIII])
         
        RI = RI_outside.index(rotorI[RII])
           
        ref = reflector[RI]

        b_RI = rotorI.index(RI_outside[ref])
           
        b_RII = rotorII.index(RII_outside[b_RI])
           
        b_RIII = rotorIII.index(RIII_outside[b_RII])

        out = plugboard[b_RIII]
        output.append(out)
    output_string = [num_to_alphet[num] for num in output]
    
    return output_string

input_string = ['I','P','Q','H','U','G','C','X','Z','M']
answer = ['H','E','I','L','H','I','T','L','E','R']
final_plugboard = list()
final_rotorI = list()
final_rotorII = list()
final_rotorIII = list()
final_start = list()
rotorI = read_file('Rotor_I_web.txt')
rotorII = read_file('Rotor_II_web.txt')
rotorIII = read_file('Rotor_III_web.txt')
#arrow = read_file('Rotor_arrow_web.txt')
arrow = [22,0,0]#RotorIII 固定 
RI = read_file('RI.txt')
RII = read_file('RII.txt')
RIV = read_file('RIV.txt')
RV = read_file('RV.txt')

rotorCombined1 = []
rotorCombined1Arrow = []
rotorCombined1Sign = []
rotorCombined2 = []
rotorCombined2Arrow = []
rotorCombined2Sign = []


def randomRotor():
	rotors = [['RI',RI,17],['RII',RII,5],['RIV',RIV,10],['RV',RV,0]]
	result = list(permutations(rotors,2))
	for i in range(6):
		rotorCombined1.append([result[i][0][1],result[i][1][1]])
		rotorCombined1Sign.append([result[i][0][0],result[i][1][0]])
		rotorCombined1Arrow.append([result[i][0][2],result[i][1][2]])
	for i in range(6,12):
		rotorCombined2.append([result[i][0][1],result[i][1][1]])
		rotorCombined2Sign.append([result[i][0][0],result[i][1][0]])
		rotorCombined2Arrow.append([result[i][0][2],result[i][1][2]])

def validation():
	global rotorI, rotorII, rotorIII, arrow, input_string, answer, final_plugboard, final_start, final_rotorIII, final_rotorII, final_rotorI
	correct = False
	time_start = time.time()
	time_end = 0
	
	time_printRotorAndArrow_start = time.time()
	time_printRotorAndArrow_end = 0
	
	error=0
	while(not correct):
		plugboard = randomPlugboard()
		for i in range(26):
			for j in range(26):
				for k in range(26):
					start_position = [i,j,k]
					
					for l in range(len(rotorCombined2)):
						rotorII,rotorI = (rotorCombined2[l][0],rotorCombined2[l][1])
						arrow[1],arrow[2] = (rotorCombined2Arrow[l][0],rotorCombined2Arrow[l][1])
						

						time_printRotorAndArrow_end = time.time()
						time_pass = time_printRotorAndArrow_end - time_printRotorAndArrow_start
						if time_pass > 10:
							time_end = time.time()
							print('共經過:',round((time_end - time_start),3),'sec')
							#print(time_pass)
							print('Now RotorII: ',rotorCombined2Sign[l][0],'Now RotorI: ', rotorCombined2Sign[l][1])
							print('RotorII Arrow: ',arrow[1],' RotorI Arrow: ', arrow[2])
							time_pass = 0
							time_printRotorAndArrow_end =0
							time_printRotorAndArrow_start = time.time()
							
						if answer == enigma(plugboard,rotorIII,rotorII,rotorI, arrow, start_position, input_string):
							print("correct!")
							final_plugboard = plugboard
							final_rotorI = rotorI
							final_rotorII = rotorII
							final_rotorIII = rotorIII
							final_start = start_position
							correct = True
							time_end = time.time()
							print("[error:",error,"] [It cost %f sec to correct]" % (time_end - time_start))
							print("plugboard:",final_plugboard)
							print("rotorIII:", final_rotorIII)
							print("rotorII:", final_rotorII)
							print("rotorI:", final_rotorI)
							print("final_start:", final_start)
							break
						else:
							error+=1
							if error%1000000 == 0:
								time_end = time.time()
								print("[error:", error/1000000, "million]  ", "[sec:", (time_end - time_start),"]")
				if correct:
					break
			if correct:
				break
		if correct:
			break
'''
	
	

def validation():
    global rotorI, rotorII, rotorIII, arrow, input_string, answer, final_plugboard, final_start, final_rotorIII, final_rotorII, final_rotorI
    correct = False
    time_start = time.time()
    time_end = 0
    error = 0
    while(not correct):
        plugboard = randomPlugboard()
        for i in range(26):
            for j in range(26):
                for k in range(26):
                    start_position = [i,j,k]
                    for l in range(len(rotorCombined1)):
						rotorII,rotorI = (rotorCombined1[l][0],rotorCombined1[l][1])
						print('now RotorII and RotorI',rotorCombined1[l][0],rotorCombined1[l][1])
						if answer == enigma(plugboard,rotorIII,rotorII,rotorI, arrow, start_position, input_string):
							print("correct!")
							final_plugboard = plugboard
							final_rotorI = rotorI
							final_rotorII = rotorII
							final_rotorIII = rotorIII
							final_start = start_position
							correct = True
							time_end = time.time()
							print("[error:",error,"] [It cost %f sec to correct]" % (time_end - time_start))
							print("plugboard:",final_plugboard)
							print("rotorIII:", final_rotorIII)
							print("rotorII:", final_rotorII)
							print("rotorI:", final_rotorI)
							print("final_start:", final_start)
							break
						else:
							error += 1
							if error%1000000 == 0:
								time_end = time.time()
								print("[error:", error/1000000, "million]  ", "[sec:", (time_end - time_start),"]")
                if correct:
                    break
            if correct:
                break
        if correct:
            break
'''

if __name__ == "__main__":
	randomRotor()
	validation()