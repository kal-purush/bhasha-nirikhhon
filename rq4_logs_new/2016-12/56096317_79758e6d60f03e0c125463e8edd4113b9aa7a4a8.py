# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 22:39:30 2015

"""
from sklearn import datasets
iris = datasets.load_iris()
digits = datasets.load_digits()

from sklearn import svm
clf = svm.SVC()
iris = datasets.load_iris()
X, y = iris.data, iris.target
clf.fit(X, y)

import pickle
s = pickle.dumps(clf)
clf2 = pickle.loads(s)
clf2.predict(X[0])
array([0])
y[0]

clf = svm.SVC(gamma=0.001, C=100.)
clf.fit(digits.data[:-1], digits.target[:-1]) 
clf.predict(digits.data[-1])

