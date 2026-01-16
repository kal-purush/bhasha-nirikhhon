import numpy as np
import pandas as pd
#import pyexcel as pe
from sklearn import preprocessing
from sklearn.decomposition import PCA
from sklearn import svm
#from sklearn.neural_network import MLPClassifier
#from sklearn.model_selection import StratifiedKFold
from sklearn import tree
from sklearn.model_selection import train_test_split
import csv

data=pd.read_csv("SF_ProcessedEightFeatures.csv",header=None)
#print("Total rows: {0}".format(len(data)))
d=data.as_matrix(columns=None)

csv_writer = csv.writer(open('SF_unique.csv','w'))

temp = np.asarray(d)
temp_y = temp[:,9]
temp_x = temp[:,0:6]

unique_x = np.empty((1,6))
unique_y = []
for i,row in enumerate(temp_x[:,:]):
	print i
	row = row.reshape(1,6)
	#if row not in unique_data:
	if not(any((row==x).all() for x in unique_x)):
		unique_x = np.append(unique_x,row,axis=0)
		unique_y.append(temp_y[i])
		if i==0:
			unique_x = np.delete(unique_x,0,axis=0)
	else:
		j = unique_x.tolist().index(row.squeeze().tolist())
		if unique_y[j] < temp_y[i]:
			unique_y[j] = temp_y[i]
unique_y = np.array(unique_y)
unique_y = unique_y.reshape(unique_y.shape[0],1)

unique_data = np.hstack((unique_x,unique_y))

for row in unique_data:
	csv_writer.writerow(row)

"""
X_train_n=temp[0:5,0:6]
y_train_n=temp[0:5,9]
if np.equal(X_train_n[1,0:6],X_train_n[4,0:6]).all():
	print 2
	if y_train_n[1]>=y_train_n[2]:
		X_train_n[2]=X_train_n[4]
		print X_train_n[2]

exit()
"""