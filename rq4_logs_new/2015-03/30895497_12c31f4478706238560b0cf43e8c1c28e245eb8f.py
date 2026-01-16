# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 13:30:23 2015

@author: Kyle, Buddy,  Igii

"""

from sklearn import cross_validation
import numpy as np

# Load in data
a = open('criticratings.p', 'rb')
b = open('audienceratings.p', 'rb')
c = open('features.npy', 'rb')
criticratings = np.array(np.load(a))
audienceratings = np.array(np.load(b))
features = np.load(c)
a.close()
b.close()
c.close()

# Create the kfolds
k = 3;
kf = cross_validation.KFold(len(features),k,shuffle=True)

# Set data as training and test data
for train_index, test_index in kf:
    trX, teX = features[train_index], features[test_index]
    trCrit, teCrit = criticratings[train_index], criticratings[test_index]
    trAud, teAud = audienceratings[train_index], audienceratings[test_index]