#!/usr/bin/env python
# coding: utf-8

#  Hyperparameter Optimization
#
# ### My dependancies: Random pandas matplotlib.pyplot numpy seaborn
#  DecisionTreeClassifier, export_graphviz CountVectorizer train_test_split
#  argparse

# Milos Milic Arzan Irani Nov 2018
#
# This script takes in the clean_breast_cancer_data_after_dropped_features.csv
# data set and tries to find the optimal max_depth and min sample split for the decision tree
#
# Usage: 02_eda_on_breast_cancer_data.py  clean_breast_cancer_data_after_dropped_features.csv

#


import argparse

import random
import pandas as pd
import matplotlib.pyplot as plt
#get_ipython().run_line_magic('matplotlib', 'inline')
import numpy as np
import seaborn as sns
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split



def main():

    # Reading in the data in from the CSV in the data folder using pandas
    breast_cancer_data = pd.read_csv("../data/clean_breast_cancer_data_after_dropped_features.csv")

    test_depths = range(1,10)

    # Let's separate the features from the target
    features_X = breast_cancer_data.drop(columns = ['id','diagnosis'])
    target_y = breast_cancer_data[['diagnosis']]

    # Let's create our train_test_split
    X_train, X_test, y_train, y_test = train_test_split(features_X, target_y, test_size=0.2, random_state = 20)


    # Now, the idea is to repeat the training and testing of the same training and testing data set for different values of the hyperparameter of **max_depth**. We'll do this by writing a loop that iterates over all the **test_depths**

    training_scores = []
    test_scores = []
    for depth in test_depths:
        breast_cancer_malignancy_classifier = DecisionTreeClassifier(max_depth = depth)
        breast_cancer_malignancy_classifier.fit(X_train, y_train)
        breast_cancer_malignancy_classifier.predict(X_train)
        training_scores.append(breast_cancer_malignancy_classifier.score(X_train, y_train))
        breast_cancer_malignancy_classifier.predict(X_test)
        test_scores.append(breast_cancer_malignancy_classifier.score(X_test, y_test))



    # Let's plot the values to see what's happening visually as well
    plt.plot(test_depths,training_scores, color = 'blue')
    plt.plot(test_depths,test_scores, color = 'green')
    plt.title("Accuracy Scores vs max_depth")
    plt.xlabel("Max Depth of Tree")
    plt.ylabel("Accuracy Scores")
    plt.legend(labels = ['training','test'])
    plt.savefig('../imgs/max_depth_graph.png')


    accuracy_data = pd.DataFrame(columns=['depth', 'training_score', 'test_score'])





    accuracy_data['depth'] = test_depths
    accuracy_data['training_score'] = training_scores
    accuracy_data['test_score'] = test_scores
    accuracy_data






    max_test_score = max(test_scores)


# After repeating the above experiment multiple times, it appears that the highest test accuracy we've been able to achieve is 93.85% while at a max depth of 5.
#
#
# From the plot above, it is also clear that the behavior of the decision tree is rather eratic, and may be we can try to adjust the min_sample_split for depth 5 to see if we can improve the test accuracy while also creating a more stable model.
#
# ## Optimum Hyperparameter | max_depth = 5
# ***

# # Hyperparameter Optimization | min_sample_split
# Let's generate an array of random split sizes

    random_min_sample_splits = np.random.randint(low=2, high=60, size=30)
    random_min_sample_splits.sort()
    random_min_sample_splits

    training_scores2 = []
    test_scores2 = []
    for val in random_min_sample_splits:
        breast_cancer_malignancy_classifier = DecisionTreeClassifier(max_depth = 5, min_samples_split = val)
        breast_cancer_malignancy_classifier.fit(X_train, y_train)
        breast_cancer_malignancy_classifier.predict(X_train)
        training_scores2.append(breast_cancer_malignancy_classifier.score(X_train, y_train))
        breast_cancer_malignancy_classifier.predict(X_test)
        test_scores2.append(breast_cancer_malignancy_classifier.score(X_test, y_test))


# Let's plot the values to see what's happening visually as well
    plt.plot(random_min_sample_splits,training_scores2, color = 'blue')
    plt.plot(random_min_sample_splits,test_scores2, color = 'green')
    plt.title("Accuracy Scores vs min_samples_split (@ depth = 7)")
    plt.xlabel("Minimum Samples Split")
    plt.ylabel("Accuracy Scores")
    plt.legend(labels = ['training','test'])
    plt.savefig('../imgs/min_sample_split_graph.png')



    accuracy_data2 = pd.DataFrame(columns = ['splits', 'training_score', 'test_score'])
    accuracy_data2['splits'] = random_min_sample_splits
    accuracy_data2['training_score'] = training_scores2
    accuracy_data2['test_score'] = test_scores2
    accuracy_data2


    #save pic


    max_test_score2 = max(test_scores2)

main()


# Considering that the min_sample_split doesn't restrict the maximum number of sample splits, I would default to a smaller value of min_sample_split to allow the model more freedom to pick the best or optimal split size. The plot shows that the accuracy drops as the min_sample_split rises, which is expected. Hence I would pick a value of 2 for the min_sample_split.
# ***
# ## Optimum Hyperparameter | min_sample_split = 2