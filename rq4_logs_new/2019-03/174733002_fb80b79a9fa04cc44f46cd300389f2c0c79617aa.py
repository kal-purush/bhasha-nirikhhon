#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 26 10:13:35 2019

@author: chaitralikshirsagar
"""
import numpy as np
import os
from scipy.ndimage import imread

# Parameters
nrun = 20 # number of classification runs
fname_label = 'class_labels.txt' # where class labels are stored for each run

def classification_run(folder,ftype='cost'):
    
    assert((ftype=='cost') |(ftype=='score'))
    data=[]
    train_files=[]
    test_files=[]
    
    fpath=os.path.join(folder,fname_label)
    for line in open(fpath,'r'):
        data.append(line.split())
        
    for i in range(nrun):
        test_files.append(data[i][0])
        train_files.append(data[i][1])
    
    target_files=train_files.copy()
    train_files=sorted(train_files)
    test_files=sorted(test_files)
    
    trainlen=len(train_files)
    testlen=len(test_files)
    
    img_test=[LoadImage('/Users/chaitralikshirsagar/Downloads/all_runs'+'/'+f)for f in test_files] 
    img_train=[LoadImage('/Users/chaitralikshirsagar/Downloads/all_runs'+'/'+f)for f in train_files]       
#    
    cost = np.zeros((testlen,trainlen))
    
    for i in range(testlen):
        for  j in range(trainlen):
            cost[i,j]=Distance_Calc(img_test[i],img_train[j])
    
    if ftype=='cost':
        predicted_y = np.argmin(cost,axis=1)
    elif ftype=='score':
        predicted_y=np.argmax(cost,axis=1)
    else:
        assert False
    
    count=0
    for i in range(testlen):
        if(train_files[predicted_y[i]]==target_files[i]):
            count=count+1
    accuracy = (count/float(testlen))
    error = 1 - accuracy
    return error*100

def LoadImage(img_label):
    
    Image=imread(img_label,flatten=True)
    return Image

def Distance_Calc(A,B):
    a1= A.reshape(1,105,105,1)
    b1=B.reshape(1,105,105,1)
    c = np.stack((a1,b1))
    c = list(c)
    p = model.predict(c)
    return p

if __name__ == "__main__":

    print ('One-shot classification demo with Modified Hausdorff Distance')
    perror = np.zeros(nrun)
    for r in range(1,nrun+1):
        rs = str(r)
        if len(rs)==1:
            rs = '0' + rs
        perror[r-1] = classification_run('/Users/chaitralikshirsagar/Downloads/all_runs/run'+rs, 'cost')
        print("run" + str(r)+ "error {:.1f}% ".format(perror[r-1]))
    total = np.mean(perror)
    print("Average error {:.1f}%".format(total))