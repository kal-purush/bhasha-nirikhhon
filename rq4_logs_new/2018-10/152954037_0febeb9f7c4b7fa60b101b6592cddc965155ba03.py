import numpy as np

#加载数据集
def loadDataSet():
	dataMat = []; labelMat = []
	fr = open('E:/python/machinelearning/机器学习实战课文相关下载/machinelearninginaction/Ch05/testSet.txt')
	for line in fr.readlines():
		lineArr = line.strip().split() #划分数据
		dataMat.append([1.0,float(lineArr[0]),float(lineArr[1])])
		labelMat.append(int(lineArr[2]))
	'''	
	print("dataMat:++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	print(dataMat)
	print("labelMat:---------------------------------------------------------------")
	print(labelMat)
	'''	
	return dataMat,labelMat

#sigmoid函数
def sigmoid(inX):
	return 1.0 / (1 + np.exp(-inX))

#梯度上升函数
def gradAscent(dataMatIn,classLabels):
	dataMatrix = np.mat(dataMatIn) #转为NUMPY矩阵
	labelMat = np.mat(classLabels).transpose() #转为NUMPY矩阵
	m,n = np.shape(dataMatrix) #读矩阵长度，m行n列
#	print("m=%d,n=%d" %(m,n))
	alpha = 0.001
	maxCycles = 500
	weights = np.ones((n,1))
	for k in range(maxCycles): #循环迭代5maxCycles次
		h = sigmoid(dataMatrix * weights) #h是列向量
	#	print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
	#	print(h)
		error = (labelMat - h)
		weights = weights + alpha * dataMatrix.transpose() * error #alpha是向目标移动的步长
	print("k=%d" % k)
	return weights

def plotBestFit(weights):
	import matplotlib.pyplot as plt 
#	weights = wei.getA()
	dataMat, labelMat = loadDataSet()
	dataArr = np.array(dataMat)
	n = np.shape(dataArr)[0]
	xcord1 = []; ycord1 = []
	xcord2 = []; ycord2 = []
	for i in range(n):
		if int(labelMat[i]) == 1:
			xcord1.append(dataArr[i,1])
			ycord1.append(dataArr[i,2])
		else:
			xcord2.append(dataArr[i,1])
			ycord2.append(dataArr[i,2])
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.scatter(xcord1,ycord1,s = 30, c = 'red', marker = 's')
	ax.scatter(xcord2,ycord2,s = 30, c = 'green')
	x = np.arange(-3.0,3.0,0.1)
	y = (-weights[0] - weights[1] * x ) / weights[2]
	ax.plot(x,y)
	plt.xlabel('X1')
	plt.ylabel('X2')
	plt.show()

#stochastic gradient ascent 0
def stocGradAscent0(dataMatrix,classLabels):
	m,n = np.shape(dataMatrix) 
	alpha = 0.01
	weights = np.ones(n)
	for i in range(m):
		h = sigmoid(sum(dataMatrix[i] * weights)) #h is a single value, not a vector
		error = classLabels[i] - h #error is a single value, not a vector
		weights = weights + alpha * error * dataMatrix[i]
	return weights

#stochastic gradient ascent 1
def stocGradAscent1(dataMatrix,classLabels, numIter = 150):
	m,n = np.shape(dataMatrix) 	
	weights = np.ones(n) #初始化为1的NUMPY矩阵
	for j in range(numIter): 
		dataIndex = list(range(m))
		for i in range(m):
			alpha = 4 / (1.0 + j + i) + 0.0001 #0.0001保证alpha不为0
			randIndex = int(np.random.uniform(0,len(dataIndex)))
			h = sigmoid(sum(dataMatrix[randIndex] * weights)) #h is a single value, not a vector
			error = classLabels[randIndex] - h #error is a single value, not a vector
			weights = weights + alpha * error * dataMatrix[randIndex]
			del(dataIndex[randIndex])
	return weights

#类别判断：>0.5类别标签为1，否则为0
def classifyVector(inX,weights):
	prob = sigmoid(sum(inX * weights))
	if prob > 0.5:
		return 1.0
	else:
		return 0.0

#
def colicTest():
	#训练集
	frTrain = open('E:/python/machinelearning/机器学习实战课文相关下载/machinelearninginaction/Ch05/horsecolicTraining.txt')
	#测试集
	frTest = open('E:/python/machinelearning/机器学习实战课文相关下载/machinelearninginaction/Ch05/horsecolicTest.txt')
	trainingSet = []; trainingLabels = []
	#格式化数据
	for line in frTrain.readlines():
		currLine = line.strip().split('\t') #划分数据
		lineArr = []
		for i in range(21):
			lineArr.append(float(currLine[i]))
		trainingSet.append(lineArr)
		trainingLabels.append(float(currLine[21]))
	trainWeights = stocGradAscent1(np.array(trainingSet),trainingLabels,500)
	errorCount = 0
	numTestVec = 0.0
	for line in frTest.readlines():
		numTestVec += 1.0
		currLine = line.strip().split('\t')
		lineArr = []
		for i in range(21):
			lineArr.append(float(currLine[i]))
		if int(classifyVector(np.array(lineArr),trainWeights)) != int(currLine[21]):
			errorCount += 1
	errorRate = (float(errorCount) / numTestVec)
	print("the error rate of this test is: %f" % errorRate)
	return errorRate

#调用colicTest()10次，然后求平均值
def multiTest():
	numTests = 10
	errorSum = 0.0
	for k in range(numTests):
		errorSum += colicTest()
	print("after %d iterations the average error rate is: %f" % (numTests, errorSum / float(numTests)))