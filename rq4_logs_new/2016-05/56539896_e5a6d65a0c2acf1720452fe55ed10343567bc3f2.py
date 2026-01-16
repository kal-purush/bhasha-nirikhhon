#!/usr/bin/env python3

# implement a Random Forest for the shelter animals classification
# for comparison with the neural network

from sklearn.ensemble import RandomForestClassifier
from sklearn import cross_validation

from animaldata import AnimalData

if __name__ == '__main__':
    animals, ids = AnimalData('data/train.csv').makeArrays()
    # the first column is the outcome label, which is what we want to predict
    trainData = animals[0:,1:]
    trainTarget = animals[0:,0]


    forest = RandomForestClassifier(n_estimators = 800, max_depth = 15)
    # forest = forest.fit(trainData, trainTarget, n_jobs = -1)
    scores = cross_validation.cross_val_score(forest, trainData, trainTarget,
                                              scoring = 'log_loss', cv = 5, n_jobs = -1)
    accuracies = cross_validation.cross_val_score(forest, trainData, trainTarget,
                                              scoring = 'accuracy', cv = 5, n_jobs = -1)
    print('Log loss: %g' % scores.mean())
    print('Accuracy: %g' % accuracies.mean())
    # forest = forest.fit(trainData, trainTarget)