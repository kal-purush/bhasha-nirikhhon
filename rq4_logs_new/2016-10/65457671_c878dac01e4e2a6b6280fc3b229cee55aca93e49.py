import os
import numpy as np

def sortDict(inputList):
	inputList.sort(key=lambda line: int(line[2]))

def writeToNewDict(filePath, matrix):
	fp = open(filePath, 'w')
	for line in matrix:
		fp.write("%s" % ','.join(line))

def readDict(filePath):
	fp = open(filePath, 'r')
	lines = fp.readlines()
	contents = []
	for line in lines:
		content = line.split(' ')
		contents.append(content)

	return contents

if (__name__=="__main__"):
	
	matrix = readDict("data_NPDoubled.txt")
	sortDict(matrix)
	filePath = 'data_NPDoubled_reordered.txt'
	writeToNewDict(filePath, matrix)

	pass