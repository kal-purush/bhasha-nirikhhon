# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 12:58:14 2015

@author: Kyle, Buddy, Igii
"""

from sklearn import linear_model, cross_validation
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
k = 5;
kf = cross_validation.KFold(len(features),k,shuffle=True)

# Learn the models
learner = linear_model.LinearRegression()
critScores = cross_validation.cross_val_score(learner, features, criticratings, scoring='mean_squared_error', cv=kf)
audScores = cross_validation.cross_val_score(learner, features, audienceratings, scoring='mean_squared_error', cv=kf)

print(-critScores)
print(-audScores)