# -*- coding: utf-8 -*-
"""
Created on Fri Feb 20 09:10:09 2015

@author: SebastienD
"""

from numpy import *
from math import log
import pandas as pd

def show_results(test_label,prediction):
    z=[]
    for i in range(0,len(test_label)):
        z.append([test_label[i],prediction[i]])
        
    ps = pd.Series([tuple(j) for j in z])
    counts = ps.value_counts()
    print counts


# chi square test
# x: features (data), y: array with the classes
def chiSQ(x, y):
	cl = unique(y) # unique number of classes
	rows = x.shape[0]
	dim = x.shape[1]
	valCHI = zeros(dim) # initialize array (vector) for the chi^2 values
	
     # For each feature compute its importance
	for d in range(dim):
		feature = x[:,d]
		vals = unique(feature)
		total = 0
		for i in range(len(vals)):
			samples_val_i = where(feature==vals[i])[0]
			for j in range(len(cl)):
				ytmp = y[samples_val_i]
				Oij = len(where(ytmp==cl[j])[0])
				samples_cl_j = where(y==cl[j])[0]
				Eij = float(len(samples_val_i)*len(samples_cl_j))/rows
				total = total + pow((Oij-Eij),2)/Eij

		valCHI[d] = total
      
	chisq = valCHI
	
	return chisq

# x: features, y: classes
def infogain(x, y):
    info_gains = zeros(x.shape[1]) # features of x
    
    # calculate entropy of the data *hy*
    # with regards to class y
    cl = unique(y)
    hy = 0
    for i in range(len(cl)):
        c = cl[i]
        py = float(sum(y==c))/len(y) # probability of the class c in the data
        hy = hy+py*log(py,2)
        
    hy = -hy
    # compute IG for each feature (columns)
    for col in range(x.shape[1]): # features are on the columns
        values = unique(x[:,col]) # the distinct values of each feature
        # calculate conditional entropy *hyx = H(Y|X)*
        hyx = 0
        for i in range(len(values)): # for all values of the feature
            f = values[i] # value of the specific feature
            yf = y[where(x[:,col]==f)] # array with the the data points index where feature i = f
            # calculate h for classes given feature f
            yclasses = unique(yf) # number of classes
            # hyx = 0; # conditional class probability initialization
            for j in range(len(yclasses)):
                yc = yclasses[j]
                pyf = float(sum(yf==yc))/len(yf) # probability calls condition on the feature value
                hyx = hyx+pyf*log(pyf,2) # conditional entropy
                
        hyx = -hyx
        # Information gain
        info_gains[col] = hy - hyx
        
    return info_gains
    