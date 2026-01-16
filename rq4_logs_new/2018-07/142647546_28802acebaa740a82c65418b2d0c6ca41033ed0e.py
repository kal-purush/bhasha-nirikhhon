# -*- coding: utf-8 -*-
"""
@author: Narges
"""

# =============================================================================
# Dont forget to pickle your classifier
# =============================================================================
import nltk
from nltk.classify.scikitlearn import SklearnClassifier
import random
import pickle
from sklearn.naive_bayes import MultinomialNB, BernoulliNB
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.svm import SVC, LinearSVC, NuSVC

from nltk.classify import ClassifierI
from statistics import mode

from nltk.tokenize import word_tokenize


class VoteClassifier (ClassifierI):
    def __init__ (self, *classifiers):
        self._classfiers = classifiers
        
    def classify (self, features):
        votes= []
        for c in self._classfiers:
            v = c.classify(features)
            votes.append(v)
        return mode (votes)
    def confidence (self, features):
        votes = []
        for c in self._classfiers:
            v = c.classify (features)
            votes.append(v)


