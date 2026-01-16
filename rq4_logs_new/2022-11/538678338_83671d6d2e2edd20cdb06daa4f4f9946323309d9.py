import matplotlib.pyplot as plt
import numpy as np


def readFile(benchmarkTextFile):
	benchmarkData = open(benchmarkTextFile, "r")
	hadoopTry1 = readData(benchmarkData)
	hadoopTry2 = readData(benchmarkData)
	hadoopTry3 = readData(benchmarkData)
	sparkTry1 = readData(benchmarkData)
	sparkTry2 = readData(benchmarkData)
	sparkTry3 = readData(benchmarkData)

def readData(benchmarkData):
	arr = np.empty(8,dtype='d')
	i = 0
	while (i!=7):
		line = benchmarkData.readline()
		values = line.split("\t")
		if values[0] == "real":
			minutes = values[1].split("m")[0]
			seconds = values[1].split("m")[1].split("s")[0]
			arr[i] = minutes * 60 + seconds
			print(minutes)
			print(seconds)
			i+=1
	print(arr)
	return arr


	
readFile("benchmarking_time_results.txt");