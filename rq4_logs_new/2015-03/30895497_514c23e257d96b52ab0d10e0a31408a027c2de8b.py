# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 12:58:14 2015

@author: Kyle, Buddy, Igii
"""

import matplotlib.pyplot as plt
import numpy as np
from FeatureExtract import extract


# Creates and trains a SVM and with cross_validation. 
# normal parameters are (5,1e-3,-1,'linear')
# Scores using MSE
def SupportVectorMachine(nFolds,tolerance,iterations, kernelType):
    from sklearn import svm, cross_validation
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
    
    # Create the SVMs. One for the critic score and one for the audience score
    criticSVM = svm.SVC(tol=tolerance, max_iter=iterations,kernel=kernelType)
    criticSVM.fit(features, criticratings)
    
    audienceSVM = svm.SVC(tol=tolerance, max_iter=iterations,kernel=kernelType)
    audienceSVM.fit(features, audienceratings)
    
    kf = cross_validation.KFold(len(features),nFolds,shuffle=True)

    # Learn the models
    critScores = cross_validation.cross_val_score(criticSVM, features, criticratings, scoring='mean_squared_error', cv=kf)
    audScores = cross_validation.cross_val_score(audienceSVM, features, audienceratings, scoring='mean_squared_error', cv=kf)
    
    return [-critScores.mean(),-audScores.mean()]

    
# Code used for indiviudal tests


featScores=[]
    
extract(title=True,studio=True,abridged_directors=True)
featScores.append(SupportVectorMachine(6,1e-3,-1,'rbf'))
    
extract(studio=True,abridged_directors=True,mpaa_rating=True)
featScores.append(SupportVectorMachine(6,1e-3,-1,'rbf'))
    
extract(genres=True,mpaa_rating=True,abridged_cast=True,runtime=True,title=True)
featScores.append(SupportVectorMachine(6,1e-3,-1,'rbf'))
    
featList = ['Title + Studio + Directors','Studio + Directors + mpaa_rating\n','Genre + mpaa_rating\n + Cast + Runtime + Title']
plt.plot(range(len(featList)), [d[0] for d in featScores], '.r-', label='Critic')
plt.legend(loc=1)
plt.xticks(range(len(featList)),featList,size='small',rotation='vertical')
plt.title('RBF SVM Critic Features w/ 6 Folds and Tolerance= 1e-3')
plt.ylabel('MSE')
plt.xlabel('Features')
plt.show()


""" Critic
featScores=[]

extract(studio=True,synopsis=True)
featScores.append(LinearRegression(5))

extract(studio=True,synopsis=True,abridged_directors=True)
featScores.append(LinearRegression(5))

extract(studio=True,synopsis=True,abridged_directors=True,abridged_cast=True)
featScores.append(LinearRegression(5))

featList = ['studio + synopsis','studio + abridged_directors\n + synopsis','studio + abridged_directors\n + synopsis + abridged_cast']
plt.plot(range(len(featList)), [d[1] for d in featScores], 'xb-', label='Critic')
plt.legend(loc=1)
plt.xticks(range(len(featList)),featList,size='small',rotation='vertical')
plt.title('Combo Critic Features w/ 5 Folds')
plt.ylabel('MSE')
plt.xlabel('Features')
plt.show()
"""