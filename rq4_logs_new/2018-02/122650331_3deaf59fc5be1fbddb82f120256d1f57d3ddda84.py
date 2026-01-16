# -*- coding: utf-8 -*-
"""
Created on Fri Nov 10 15:52:56 2017

@author: Tushar
"""
import csv
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier

def fmeasure(Y,Z):
    true_p = 0
    false_p = 0
    false_n = 0
    for i in range(len(Y)):
        if (Y[i] == 1.0 and Z[i] == 1.0):
            true_p += 1
        elif Y[i] == 0.0 and Z[i] == 1.0:
            false_p += 1
        elif Y[i] == 1.0 and Z[i] == 0.0:
            false_n += 1
    pre = true_p/(true_p + false_p)
    rec = true_p/(true_p + false_n)
    f_meas = (2*pre*rec)/(pre + rec)
    return f_meas
    
with open('chronic_kidney_disease_full.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    data = []
    file = []
    
    for row in readCSV:
        for i in range(0,25):
            dataset = row[i]
            data.append(dataset)
        file.append(data)
        data = []
    
    for i in range(1, len(file)):
        for j in range(25):
            if file[i][j] == 'ckd':
                file[i][j] = 1.0
            elif file[i][j] == 'notckd':
                file[i][j] = 0.0
            elif file[i][j] == 'normal':
                file[i][j] = 1.0
            elif file[i][j] == 'abnormal':
                file[i][j] = 0.0
            elif file[i][j] == 'present':
                file[i][j] = 1.0
            elif file[i][j] == 'notpresent':
                file[i][j] = 0.0
            elif file[i][j] == 'yes':
                file[i][j] = 1.0
            elif file[i][j] == 'no':
                file[i][j] = 0.0
            elif file[i][j] == 'good':
                file[i][j] = 1.0
            elif file[i][j] == 'poor':
                file[i][j] = 0.0
            else:
                file[i][j] = float(file[i][j])
    class_1 = []
    class_2 = []
    for i in range(1, len(file)):
        if file[i][24] == 1.0:
            class_1.append(file[i])
        else:
            class_2.append(file[i])
    c_train = class_1[0:int(0.8*len(class_1))]
    c_train = c_train + class_2[0:int(0.8*len(class_2))]
    c_test = class_1[(int(0.8*len(class_1))):len(class_1)+1]
    c_test = c_test + class_2[(int(0.8*len(class_2))):len(class_2)+1]
    X_train = []
    Y_train = []
    X_test = []
    Y_test = []
    for i in range(len(c_train)):
        X_train.append(c_train[i][0:24])
        Y_train.append(c_train[i][24])
    for i in range(len(c_test)):
        X_test.append(c_test[i][0:24])
        Y_test.append(c_test[i][24])
###########################################################################
# SVC LINEAR KERNEL
###########################################################################
    svc_trn = SVC(kernel = 'linear')
    svc_trn.fit(X_train, Y_train)   
    Z_svc_trn = svc_trn.predict(X_train)
    fdgh = Z_svc_trn
    f_svc_trn = fmeasure(Y_train,Z_svc_trn)
    print("F-measure for SVC using linear kernel on training set:",f_svc_trn)
    
    svc_tst = SVC(kernel = 'linear')
    svc_tst.fit(X_train, Y_train)   
    Z_svc_tst = svc_tst.predict(X_test)
    f_svc_tst = fmeasure(Y_test,Z_svc_tst)
    print("F-measure for SVC using linear kernel on test set:",f_svc_tst)
###########################################################################
# SVC RBF KERNEL
###########################################################################
    svc_trn = SVC(kernel = 'rbf')
    svc_trn.fit(X_train, Y_train)   
    Z_svc_trn = svc_trn.predict(X_train)
    f_svc_trn = fmeasure(Y_train,Z_svc_trn)
    print("F-measure for SVC using rbf kernel on training set:",f_svc_trn)
    
    svc_tst = SVC(kernel = 'rbf')
    svc_tst.fit(X_train, Y_train)   
    Z_svc_tst = svc_tst.predict(X_test)
    f_svc_tst = fmeasure(Y_test,Z_svc_tst)
    print("F-measure for SVC using rbf kernel on test set:",f_svc_tst)
###########################################################################
# RANDOM FOREST
###########################################################################
    rf_trn = RandomForestClassifier()
    rf_trn.fit(X_train, Y_train)
    Z_rf_trn = rf_trn.predict(X_train)
    f_svc_trn = fmeasure(Y_train,Z_rf_trn)
    print("F-measure for Random Forest on training set:",f_svc_trn)
    
    rf_tst = RandomForestClassifier()
    rf_tst.fit(X_train, Y_train)   
    Z_rf_tst = rf_tst.predict(X_test)
    f_svc_tst = fmeasure(Y_test,Z_rf_tst)
    print("F-measure for Random Forest on test set:",f_svc_tst)