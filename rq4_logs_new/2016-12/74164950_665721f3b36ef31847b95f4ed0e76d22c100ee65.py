###########################################
# Robert Dickerson
# Brother Burton
# CS 450 Machine Learning
###################################################

import sys

import numpy as np
import pandas
from sklearn import datasets
from sklearn.cross_validation import train_test_split as tsp


def getKAmount():
    k = 0
    while k < 1:
        k = int(input("Please enter the nearest neighbor you want the program to search to: "))

    return k



class KNN:

    def predict(self, train_data, train_target, test_data, k):


        nInputs = np.shape(test_data)[0]
        closest = np.zeros(nInputs)

        for n in range(nInputs):

            # Compute distances
            distances = np.sum((train_data-test_data[n,:])**2, axis=1)

            indices = np.argsort(distances,axis=0)

            classes = np.unique(train_target[indices[:k]])
            if len(classes)==1:
                closest[n] = np.unique(classes)
            else:
                counts = np.zeros(max(classes)+1)
                for i in range(k):
                    counts[train_target[indices[i]]] += 1
                closest[n] = np.max(counts)

        return closest



    def train(self, data_set, target_set):
        self.trainingData = np.asarray(data_set)
        self.testingData = np.asarray(target_set)
        print("\nThe system has been trained on the new set of data.\n")


def get_accuracy(results_of_predict, test_targets):
    value_correct = 0
    for i in range(test_targets.size):
        value_correct += results_of_predict[i] == test_targets[i]

    print("The system correctly predicted ", value_correct, " of ", test_targets.size,
          ". \nThe system was able to correctly predict ",
          "{0:.2f}% of the time!".format(100 * (value_correct / test_targets.size)), sep="")

def train_system(data, target, classifier):
    #random.shuffle(iris.data)
    testAmount = float(0.3)
    timesShuffled = 15
    k = getKAmount()

    train_data, test_data, train_target, test_target = tsp(data, target, test_size = testAmount,
                                                           random_state = timesShuffled)

    classifier.train(train_data, train_target)
    get_accuracy(classifier.predict(train_data, train_target, test_data, k), test_target)

def main(argv):
    number = 0

    knn = KNN()
    while number != 1 or number != 2 or number != 3:
        print ("\nChoose the Data you would like to use\n"
               "To view Iris Prediction,          enter 1\n"
               "To view Cars Prediction,          enter 2\n"
               "To view Breast Cancer Prediction, enter 3")

        number = int(input("Choice: "))

        if (number == 1):
            irisData = datasets.load_iris()
            trainData = irisData.data
            targetData = irisData.target
            train_system(trainData, targetData, knn)

        #not sure why but it doesnt want to load my csv
        if (number == 2):
            carData = pandas.read_csv("cardata.csv")
            carData = carData.values
            trainData, targetData = carData[:, :114], carData[:, 114]
            #trainData = carData[['first', 'second', 'third', 'fourth', 'fifth', 'sixth']]
            #print (carData.values)
            #print (trainData)
            #targetData = carData['target']
            train_system(trainData, targetData, knn)

        if (number == 3):
            breastCancerData = datasets.load_breast_cancer()
            trainData = breastCancerData.data
            targetData = breastCancerData.target
            train_system(trainData, targetData, knn)

if __name__ == "__main__":
    main(sys.argv)

"""
import pandas
from sklearn import cross_validation
from sklearn.ensemble import BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
url = "https://archive.ics.uci.edu/ml/machine-learning-databases/pima-indians-diabetes/pima-indians-diabetes.data"
names = ['preg', 'plas', 'pres', 'skin', 'test', 'mass', 'pedi', 'age', 'class']
dataframe = pandas.read_csv(url, names=names)
array = dataframe.values
X = array[:,0:8]
Y = array[:,8]
num_folds = 10
num_instances = len(X)
seed = 7
kfold = cross_validation.KFold(n=num_instances, n_folds=num_folds, random_state=seed)
cart = DecisionTreeClassifier()
num_trees = 100
model = BaggingClassifier(base_estimator=cart, n_estimators=num_trees, random_state=seed)
results = cross_validation.cross_val_score(model, X, Y, cv=kfold)
print(results.mean())


from sklearn import datasets, preprocessing
from sklearn.neighbors import KNeighborsClassifier
from sklearn.cross_validation import train_test_split as tsp
from sklearn.ensemble import BaggingClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn import tree
from sklearn.naive_bayes import GaussianNB
import pandas

timesShuffled = 4

#penData = pandas.read_csv("pendigits.csv")
#penData = penData.values
#trainData, targetData = penData[:, :16], penData[:, 16]
#indianData = pandas.read_csv("ILPD.csv")
#indianData = indianData.values
#trainDataX, targetDataY = indianData[:, :10], indianData[:, 10]
#wcData = pandas.read_csv("WCData.csv")
#wcData = wcData.values
#trainData, targetData = wcData[:, :6], wcData[:, 6]
abaloneData = pandas.read_csv("abalone.csv")
abaloneData = abaloneData.values
trainData, targetData = abaloneData[:, :7], abaloneData[:, 7]
testAmount = (0.3)
train_data, test_data, train_target, test_target = tsp(trainData, targetData, test_size = testAmount,
                                                           random_state = timesShuffled)
#classifier = BaggingClassifier(KNeighborsClassifier(), max_samples=0.5, max_features=0.5)
#classifier = KNeighborsClassifier(n_neighbors=3)
#classifier.fit(train_data, train_target)
#classifier = RandomForestClassifier(n_estimators=10)
#classifier = classifier.fit(test_data, test_target)
#classifier = AdaBoostClassifier(n_estimators=100)
#classifier = classifier.fit(test_data, test_target)
#classifier = tree.DecisionTreeClassifier()
#classifier = classifier.fit(test_data, test_target)
classifier = GaussianNB()
classifier = classifier.fit(test_data, test_target)
predictions = classifier.predict(test_data)
print(predictions)

value_correct = 0
for i in range(test_target.size):
    value_correct += predictions[i] == test_target[i]

print("The system correctly predicted ", value_correct, " of ", test_target.size,
    ". \nThe system was able to correctly predict ",
    "{0:.2f}% of the time!".format(100 * (value_correct / test_target.size)), sep="")"""