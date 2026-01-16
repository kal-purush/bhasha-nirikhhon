# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 18:18:56 2016

@author: hangmengyu
"""

import csv
import matplotlib.pyplot as plt
'''import seaborn as sns'''
import pandas as pd
import numpy as np
from sklearn import svm, datasets, metrics, linear_model
from sklearn.linear_model import LinearRegression
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestRegressor

def get_data():  
    data_1 = pd.read_csv('/Users/hangmengyu/Desktop/EC601/project/training.csv') #here ,use pandas to read cvs file.  
    data=np.array(data_1)
     #print(pd.DataFrame.apply(n,axis=0))
    print(data.shape)
    x= data[1:1101,1:101]
    '''sns.pairplot(data,x_vars=['','',''],y_vars='trainingdata',size= , aspect= )
    feature_cols=['','','']
    x = data[feature_cols]'''
    y = data[1:1101,3599:3600]
    x_train,x_test, y_train, y_test = train_test_split(x, y, random_state=1)
    print (x_train.shape)
    print (x_test.shape)
    print (y_train.shape)
    print (y_test.shape)  
    linreg = linear_model.LinearRegression()  
    model = linreg.fit(x_train, y_train)   
    print (model)#train model  
    predict_outcome = linreg.predict(x_test)  
    predictions = {}  
    predictions['intercept'] = linreg.intercept_  
    predictions['coefficient'] = linreg.coef_  
    predictions['predicted_value'] = predict_outcome  
    '''zip[feature_cols,linreg.coef_]'''
    print (predictions)
    print (predict_outcome.shape)
    sum_mean = 0
    for i in range(len(predict_outcome)):
        sum_mean += (predict_outcome[i]-y_test[i])**2  
    #sum_error=np.sqrt(sum_mean/predict_outcome.shape[0])
    print(sum_mean)
    print((sum_mean/predict_outcome.shape[0])**0.5)
    #plt.scatter(x_train,y_train,)

def randomforest():
   data = pd.read_csv('/Users/hangmengyu/Desktop/EC601/project/training.csv')
   data = np.array(data)
   x = data[1:1101,1:101]
   train_y = data[1:1101,3599:3600]
   y = train_y.ravel()
   train_y = np.array(y).astype(int)
   realtest = pd.read_csv('/Users/hangmengyu/Desktop/EC601/project/sorted_test.csv')
   realtest = np.array(realtest)
   model = RandomForestRegressor(n_estimators=100, min_samples_split=1)
   model.fit(x, train_y)
   #predicted_probs = model.predict(realtest)
   #print (model.predict(realtest))
   print(model.predict(x))
   print (model.score(x,train_y))
   #csv_io.write_delimited_file("random_forest_solution.csv", predicted_probs)

   
    
get_data()
#randomforest()