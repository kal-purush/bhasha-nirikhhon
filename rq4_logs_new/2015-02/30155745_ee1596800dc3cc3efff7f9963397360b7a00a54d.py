import csv, random, math, operator

trainUni = {}
trainBi = {}

def loadDataset(filename, split, trainingSet=[] , testSet=[]):
# this function loads the dataset in cvs format into a list of list and split it into
# test and train data set. The split is controlled by the parameter passed to the function
        missingout = open('missing-out.txt','w')
        with open(filename, encoding='UTF-8-sig') as csvfile:
                lines = csv.reader(csvfile)
                dataset = list(lines) # dataset is a list of lines

                for x in range(len(dataset)-1):
                        if ' ?' in dataset[x]: #write missing values to a file
                                #print ('missing value:', dataset[x])
                                missingout.write(str(dataset[x]) + '\n')
                        else:
                                if random.random() < split:
                                        trainingSet.append(dataset[x])
                                else:
                                        testSet.append(dataset[x])

def trainModel (dataset):
        global trainUni, trainBi
        trainUni['VOCSIZE'] = 0

# reads through all records in the training dataset and deletermines the following
# frequencies:
# no of occurrances for a class and total numbe of occurances - this gives us class prior
# no of times a word appears in a class - this gives data conditional probability of the word

        for rec in dataset:
                classMarker = rec[len(rec) - 1]
                for w in rec:
                        #total number time a word appear in a class
                        biw = classMarker + " " + w  
                        if biw in trainBi:
                                trainBi[biw] += 1
                        else:
                                trainBi[biw] = 1
                
                        #total number time a word appears in all classes
                        # for each unique word, increase the vocabulary size by 1
                        if w in trainUni:
                                trainUni[w] += 1
                        else:
                                trainUni[w] = 1
                                trainUni['VOCSIZE'] += 1
                
def classify(dataset, trainingTot):
        global trainUni, trainBi
        class1 = ' >50K'
        class2 = ' <=50K'
        accuracy = 0
    
        for rec in dataset:
            class1Prob = math.log(trainUni[class1]/trainingTot) # class prior for class 1
            class2Prob = math.log(trainUni[class2]/trainingTot) # class prion for class 2
            trainingClass = rec[len(rec) - 1]
            
            # assert that class 1 + class 2 should be equal to total training data set
            assert ((trainUni[class1] + trainUni[class2]) == trainingTot) 

            for item in range(len(rec) - 2):
                    w_class1 = class1 + " " + rec[item]
                    class1Prob = class1Prob + \
                                math.log((trainBi.get(w_class1,0) + 1) \
                                 /(trainUni.get(rec[item],0) + trainUni['VOCSIZE']))

                    w_class2 = class2 + " " + rec[item]
                    class2Prob = class2Prob + \
                                math.log((trainBi.get(w_class2,0) + 1) \
                                 /(trainUni.get(rec[item],0) + trainUni['VOCSIZE']))

            if abs(class1Prob) < abs(class2Prob):
                    predictedClass = class1
            else :
                    predictedClass = class2
                    
            if predictedClass == trainingClass:
                    accuracy += 1
                                    
        print ('train data size', trainingTot, 'test data size ', len(dataset), 'accuracy', \
               accuracy, 'accuracy percent', math.ceil((accuracy/len(dataset))*100),'%')
        return (math.ceil((accuracy/len(dataset))*100))
                
def bayesian_discrete():
	# prepare data
	trainingSet=[]
	testSet=[]
	split = 0.67
	loadDataset('adult-discrete.csv', split, trainingSet, testSet)
        # train data
	trainModel(trainingSet)
        # classify data
	accuracy = classify(testSet, len(trainingSet))
	return accuracy
	
	
def main(k):
        global trainUni, trainBi
        accuracyList = []
        for i in range(k):
                trainUni = {}
                trainBi = {}
                accuracy = bayesian_discrete()
                accuracyList.append(math.ceil(accuracy))
        mu = (sum (x for x in accuracyList))/len(accuracyList)
        sigma = math.sqrt((sum(pow((x - mu),2) for x in accuracyList))/len(accuracyList))
        print ('\nMean Accuracy:', mu, 'Standard Deviation', sigma)

main(10)

