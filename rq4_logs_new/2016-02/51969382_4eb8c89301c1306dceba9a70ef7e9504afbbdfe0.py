import sys
# ---- Start Functions ----#

# some funcs - some cool functionscat 
def convertA(line):
	global symbolPos
	# 'a' in symbol 		==== is a a key in symbols
	# string.isdigit()		==== is string only number
	re="0"
	line = line[1:]
	if(line in symbols):
		n=symbols[line]
	elif(line.isdigit()):
		#convert number to 15 bit address
		n=int(line)
	else:
		symbols[line] = symbolPos
		n=symbolPos
		symbolPos+=1
	add = bin(n)[2:].zfill(15)
	re+=add
	#print re
	return re

#HOPE THIS IS ALL CORRECT!
computations = {"0"  :"0101010","1"  :"0111111","-1" :"0111010","D"  :"0001100",
				"A"  :"0110000","!D" :"0001101","!A" :"0110001","-D" :"0001111",
				"-A" :"0110011","D+1":"0011111","A+1":"0110111","D-1":"0001110",
				"A-1":"0110010","D+A":"0000010","D-1":"0001110","A-D":"0000111",
				"D&A":"0000000","D|A":"0010101","M"  :"1110000","!M" :"1110001",
				"-M" :"1110011","M+1":"1110111","M-1":"1110010","D+M":"1000010",
				"M-D":"1000111","D-M":"1010011","D&M":"1000000","D|M":"1010101"}

destinations = {"null":"000","M":"001","D":"010","MD":"011",
			   "A":"100","AM":"101","AD":"110","AMD":"111"}
jumps = {"null":"000","JGT":"001","JEQ":"010","JGE":"011",
		  "JLT":"100","JNE":"101","JLE":"110","JMP":"111"}
def convertC(line):
	#111cCCCCCCDDDJJJ
	re = "111"
	computation=""
	destination=""
	jmp=""
	equals = line.find("=")
	semiCol = line.find(";")
	if(equals == -1 and semiCol == -1):
		destination="000"
		jmp="000"
		computation=computations[line]
	elif(equals != -1 and semiCol== -1):
		destination = destinations[line[:equals]]
		jmp="000"
		computation=computations[line[equals+1:]]
	elif(equals == -1 and semiCol!= -1):
		destination = "000"
		jmp=jumps[line[semiCol+1:]]
		computation=computations[line[:semiCol]]
	else:
		destination = destinations[line[:equals]]
		jmp=jumps[line[semiCol+1:]]
		computation=computations[line[equals+1:semiCol]]
	re+=computation
	re+=destination
	re+=jmp
	print re
	return re

# ---- End Functions ---- #


fileName = sys.argv[1]
print fileName
inFile = open(fileName, 'r')
instructions = list()
symbolPos = 16;
#instructions.append("Blank first instruction for index?")
symbols={'R0': 0, 'R1': 1, 'R2': 2, 'R3': 3, 'R4': 4, 'R5': 5, 'R6': 6, 'R7': 7,'R8': 8,
		 'R9': 9, 'R10': 10, 'R11': 11, 'R12': 12, 'R13': 13, 'R14': 14,'R15': 15,'SP': 0,
		 'LCL': 1,'ARG': 2, 'THIS': 3,'THAT': 4, 'SCREEN': 16384,'KBD': 24576}
lineNum=0
for line in inFile:
	#removes whitespaces
	line = line.replace(" ", "").strip('\t\n\r')
	if len(line)==0:
		continue
	n =  line.find("//")
	if n==0:
		#print "Line is comment"
		continue
	elif n==-1:
		#print "Line has no comments: ", line,
		line = line
	else:
		line = line[:n]
		#print "Line has comment: ", line[:n]
	if(line[0]=='(' and line[len(line)-1]==')'):
		symbols[line[1:len(line)-1]] = lineNum
		#print line[1:len(line)-1], " is a label @ ",lineNum+1
		continue

	instructions.append(line)
	lineNum+=1
	#print line, lineNum
#print lineNum, " instructions"
print instructions, len(instructions),lineNum
#print "\n"

period = fileName.rfind(".")
outFileName = fileName[0:period] + "_ps.hack"
print outFileName
outFile = open(outFileName, 'w')

for instruction in instructions:
	binaryInstruction = 8
	if(instruction[0]=='@'):
		binaryInstruction = convertA(instruction)
	else:
		binaryInstruction = convertC(instruction)
	outFile.write(binaryInstruction + "\n")
print "We did it!"
    