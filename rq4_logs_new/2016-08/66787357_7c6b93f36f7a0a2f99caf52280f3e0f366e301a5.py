 
import csv
import random
import math
import operator
 

def euclideanDistance(instance1, instance2, length):
        distance = 0
        for x in range(length):
                distance += pow((float(instance1[x]) - float(instance2[x])), 2)
        return math.sqrt(distance)
 
def getNeighbors(trainingSet, testInstance, k):
        distances = []
        length = len(testInstance)-1
        for x in range(len(trainingSet)):
                dist = euclideanDistance(testInstance, trainingSet[x], length)
                distances.append((trainingSet[x], dist))
        distances.sort(key=operator.itemgetter(1))
        neighbors = []
        for x in range(k):
                neighbors.append(distances[x][0])
        return neighbors
 
def getResponse(neighbors):
        classVotes = {}
        for x in range(len(neighbors)):
                response = neighbors[x][-1]
                if response in classVotes:
                        classVotes[response] += 1
                else:
                        classVotes[response] = 1
        sortedVotes = sorted(classVotes.iteritems(), key=operator.itemgetter(1), reverse=True)
        return sortedVotes[0][0]
 
def getAccuracy(testSet, predictions):
        correct = 0
        for x in range(len(testSet)):
                if testSet[x][-1] == predictions[x]:
                        correct += 1
        return (correct/float(len(testSet))) * 100.0

def main():
        
        trainingSet=[]
        testSet=[]
        data1=[]
        data2=[]
        data3=[]
        with open('1.data', 'rb') as csvfile:
            lines = csv.reader(csvfile, delimiter='	')
            dataset1 = list(lines)
            for x in range(len(dataset1)):
                data1.append(dataset1[x])
        with open('2.data', 'rb') as csvfile:
            lines = csv.reader(csvfile, delimiter='	')
            dataset1 = list(lines)
            for x in range(len(dataset1)):
                data2.append(dataset1[x])

        with open('3.data', 'rb') as csvfile:
            lines = csv.reader(csvfile, delimiter='	')
            dataset1 = list(lines)
            for x in range(len(dataset1)):
                data3.append(dataset1[x])
 
        
        
        print 'Training set: ' + repr(len(trainingSet))
       
        print 'set: ' + repr(len(data1))
        print 'set: ' + repr(len(data2))
        print 'set: ' + repr(len(data3))

        for x in range(len(data1)):
                for y in range(13):
                    data1[x][y] = float(data1[x][y])
        for x in range(len(data2)):
                for y in range(13):
                    data2[x][y] = float(data2[x][y])                
        for x in range(len(data3)):
                for y in range(13):
                    data3[x][y] = float(data3[x][y])
        taxa=0
        for i in range (10):
                trainingSet=[]
                testSet=[]
                for x in range(len(data1)):
                      if((x>=(0.1*len(data1))*i)) & (x<(0.1*(len(data1))*(i+1))):
                            testSet.append(data1[x])
                      else:
                            trainingSet.append(data1[x])
                for x in range(len(data2)):
                      if((x>=(0.1*len(data2))*i)) & (x<(0.1*(len(data2))*(i+1))):
                            testSet.append(data2[x])
                      else:
                            trainingSet.append(data2[x])
                for x in range(len(data3)):
                      if((x>=(0.1*len(data3))*i)) & (x<(0.1*(len(data3))*(i+1))):
                            testSet.append(data3[x])
                      else:
                            trainingSet.append(data3[x])
                               
              
                print "Treino " + str(len(trainingSet))
                print "Teste "+ str(len(testSet))
                predictions=[]
                k = 1
                for x in range(len(testSet)):
                       neighbors = getNeighbors(trainingSet, testSet[x], k)
                       result = getResponse(neighbors)
                       predictions.append(result)
                       #print('> predicted=' + repr(result) + ', actual=' + repr(test[x][-1]))
                accuracy = getAccuracy(testSet, predictions)
                print("fold "+str(i)+'    Accuracy:' + repr(accuracy) + '%')
                taxa+=accuracy
        print taxa/10
main()

