from math import log


#Calculation Shannon entropy
def calcShannonEnt(dataSet):
    numEntries = len(dataSet)
    labelCounts = {}
    for feature in dataSet:
        currentLabel = feature[-1]
        if currentLabel not in labelCounts.key():
            labelCounts[currentLabel] = 0
        labelCounts[currentLabel] += 1
    shannonEnt = 0.0
    for key in labelCounts:
        prob = float(labelCounts[key])/numEntries
        shannonEnt -= prob * log(prob,2)
    return shannonEnt



#Split data set
def splitDataSet(dataSet,axis,value):
    retDataSet = []
    for feature in dataSet:
        if feature[axis] == value:
            reducedFeature = feature[:axis]
            reducedFeature.extend(feature[axis+1:])
            retDataSet.append(reducedFeature)
    return retDataSet


#According to Shannon entropy, select the best feature to split the dataset.
def chooseBestFeatureToSplit(dataSet):
    numFeatures = len(dataSet[0]) - 1
    baseEntropy = calcShannonEnt(dataSet)
    bestInfoGain = 0.0
    bestFeature = -1
    for i in range(numFeatures):
        featList = [example[i] for example in dataSet]
        uniqueVals = set(featList)
        newEntropy = 0.0
        for value in uniqueVals:
            subDataSet = splitDataSet(dataSet,i,value)
            prob = len(subDataSet)/float(len(dataSet))
            newEntropy += prob * calcShannonEnt(subDataSet)
        infoGain = baseEntropy - newEntropy
        if infoGain > bestInfoGain:
            bestInfoGain = infoGain
            bestFeature = i
    return bestFeature