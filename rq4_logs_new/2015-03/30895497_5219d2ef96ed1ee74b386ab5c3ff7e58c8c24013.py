# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 12:58:14 2015

@author: Kyle, Buddy, Igii
"""

import matplotlib.pyplot as plt
import numpy as np

# Creates and trains a linear model and with cross_validation
# Scores using MSE
def LinearClassification(nFolds):
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
    kf = cross_validation.KFold(len(features),nFolds,shuffle=True)
    
    # Learn the models
    learner = linear_model.SGDClassifier()
    critScores = cross_validation.cross_val_score(learner, features, criticratings, scoring='mean_squared_error', cv=kf)
    audScores = cross_validation.cross_val_score(learner, features, audienceratings, scoring='mean_squared_error', cv=kf)
    
    return [-critScores.mean(),-audScores.mean()]

# Use this to find the best fold when testing individual features
def findBestFoldAcrossIndividFeatures(maxFold=6):
    meanScores = []
    for i in range(2,maxFold):
        nFolds = i
        featScores = []
       
        extract(runtime=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(year=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(month=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(mpaa_rating=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(genres=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(studio=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(abridged_directors=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(title=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(abridged_cast=True)
        featScores.append(LinearClassification(nFolds))  
        
        extract(synopsis=True)     
        featScores.append(LinearClassification(nFolds))  
          
        meanScores.append(np.mean(featScores))  
          
        # Plot the graph with using all the features
        featList = ['runtime','year','month','mpaa_rating','genres','studio','abridged_directors','title','abridged_cast','synopsis']
        plt.plot(range(len(featList)), [d[0] for d in featScores], 'xb-', label='Critic')
        plt.plot(range(len(featList)), [d[1] for d in featScores], '.r-', label='Audience')
        plt.legend(loc=1)
        plt.xticks(range(len(featList)),featList,size='small',rotation='vertical')
        plt.title('Individual Features w/ ' + str(nFolds) + ' Folds')
        plt.ylabel('MSE')
        plt.xlabel('Features')
        plt.show()
        
    return meanScores.index(min(meanScores)) + 2
    
# Code used for indiviudal tests
""" Audience
    featScores=[]
    
    extract(genres=True,synopsis=True)
    featScores.append(LinearClassification(5))
    
    extract(genres=True,synopsis=True,mpaa_rating=True)
    featScores.append(LinearClassification(5))
    
    extract(genres=True,synopsis=True,mpaa_rating=True,runtime=True)
    featScores.append(LinearClassification(5))
    
    featList = ['genres + synopsis','genres + mpaa_rating\n + synopsis','genres + mpaa_rating\n + synopsis + runtime']
    plt.plot(range(len(featList)), [d[1] for d in featScores], '.r-', label='Audience')
    plt.legend(loc=1)
    plt.xticks(range(len(featList)),featList,size='small',rotation='vertical')
    plt.title('Combo Audience Features w/ 5 Folds')
    plt.ylabel('MSE')
    plt.xlabel('Features')
    plt.show()
"""

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