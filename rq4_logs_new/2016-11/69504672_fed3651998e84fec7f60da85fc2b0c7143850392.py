# -*- coding: utf-8 -*-
"""
Created on Sat Oct 22 11:49:22 2016

@author: hangmengyu
"""


import pandas as pd
import numpy as np
from sklearn import svm, datasets, metrics, linear_model
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR

def get_data():  
    data_1 = pd.read_csv('/Users/hangmengyu/Desktop/EC601/project/training.csv') #here ,use pandas to read cvs file.  
    data=np.array(data_1)
     #print(pd.DataFrame.apply(n,axis=0))
    print(data.shape)
    x= data[1:1101,1:3500]
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
    print(sum_mean)
    print((sum_mean/predict_outcome.shape[0])**0.5)

def randomforest():
    data_1 = pd.read_csv('/Users/hangmengyu/Desktop/EC601/project/training.csv') #here ,use pandas to read cvs file.  
    data=np.array(data_1)
     #print(pd.DataFrame.apply(n,axis=0))
    print(data.shape)
    x= data[1:1101,1:3500]
    
    y = data[1:1101,3599:3600]
    y = y.ravel()
    x_train,x_test, y_train, y_test = train_test_split(x, y, random_state=1)
    print (x_train.shape)
    print (x_test.shape)
    print (y_train.shape)
    print (y_test.shape)  
    ranfr = RandomForestRegressor(n_estimators=100, min_samples_split=1)  
    model = ranfr.fit(x_train, y_train)   
    print (model)#train model  
    predict_outcome = ranfr.predict(x_test)  
    predictions = {}  
    #predictions['intercept'] = ranfr.intercept_  
    #predictions['coefficient'] = ranfr.coef_  
    predictions['predicted_value'] = predict_outcome  
    #zip[feature_cols,ranfr.coef_]
    print (predictions)
    print (predict_outcome.shape)
    sum_mean = 0
    for i in range(len(predict_outcome)):
        sum_mean += (predict_outcome[i]-y_test[i])**2  
    #sum_error=np.sqrt(sum_mean/predict_outcome.shape[0])
    print(sum_mean)
    print((sum_mean/predict_outcome.shape[0])**0.5)
    
def svmdata():
    data_1 = pd.read_csv('/Users/hangmengyu/Desktop/EC601/project/training.csv') #here ,use pandas to read cvs file.  
    data=np.array(data_1)
     #print(pd.DataFrame.apply(n,axis=0))
    print(data.shape)
    x= data[1:1101,1:3500]
    '''sns.pairplot(data,x_vars=['','',''],y_vars='trainingdata',size= , aspect= )
    feature_cols=['','','']
    x = data[feature_cols]'''
    y = data[1:1101,3599:3600]
    y = y.ravel()
    x_train,x_test, y_train, y_test = train_test_split(x, y, random_state=1)
    svr_rbf = SVR(kernel='rbf')
    svr_lin = SVR(kernel='linear')
    svr_poly = SVR(kernel='poly', degree=2)
    y_rbf = svr_rbf.fit(x_train, y_train).predict(x_test)
    y_lin = svr_lin.fit(x_train, y_train).predict(x_test)
    y_poly = svr_poly.fit(x_train, y_train).predict(x_test)
    sum_mean = 0
    print(y_poly.shape)
    for i in range(len(y_poly)):
        sum_mean += (y_poly[i]-y_test[i])**2  
    #sum_error=np.sqrt(sum_mean/predict_outcome.shape[0])
    print(sum_mean)
    print((sum_mean/y_poly.shape[0])**0.5)
    sum_mean = 0
    for i in range(len(y_rbf)):
        sum_mean += (y_rbf[i]-y_test[i])**2  
    #sum_error=np.sqrt(sum_mean/predict_outcome.shape[0])
    print(sum_mean)
    print((sum_mean/275)**0.5)
    sum_mean = 0
    for i in range(len(y_lin)):
        sum_mean += (y_lin[i]-y_test[i])**2  
    #sum_error=np.sqrt(sum_mean/predict_outcome.shape[0])
    print(sum_mean)
    print((sum_mean/275)**0.5)    
    
#get_data()
randomforest()
#svmdata()