import pandas as pd
import os
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import make_scorer, accuracy_score
from sklearn.model_selection import train_test_split

def classEncoding(val):
	if(val == 1):
		return 0,0,0,0,0,0,0,0,1
	elif (val == 2):
		return 0,0,0,0,0,0,0,1,0
	elif (val == 3):
		return 0,0,0,0,0,0,1,0,0
	elif (val == 4):
		return 0,0,0,0,0,1,0,0,0
	elif (val == 5):
		return 0,0,0,0,1,0,0,0,0
	elif (val == 6):
		return 0,0,0,1,0,0,0,0,0
	elif (val == 7):
		return 0,0,1,0,0,0,0,0,0
	elif (val == 8):
		return 0,1,0,0,0,0,0,0,0
	else:
		return 1,0,0,0,0,0,0,0,0
	return 1,0,0,0,0,0,0,0,0	



def modTrain(df):
	x = df.Class.apply(lambda x: classEncoding(x))
	print type(x)	
	df['Prediction1'] = x.map(lambda x: x[8])
	df['Prediction2'] = x.map(lambda x: x[7])
	df['Prediction3'] = x.map(lambda x: x[6])
	df['Prediction4'] = x.map(lambda x: x[5])
	df['Prediction5'] = x.map(lambda x: x[4])
	df['Prediction6'] = x.map(lambda x: x[3])
	df['Prediction7'] = x.map(lambda x: x[2])
	df['Prediction8'] = x.map(lambda x: x[1])
	df['Prediction9'] = x.map(lambda x: x[0])
	return df
pass

def redundantFeature(lst):
	tempLst = []
	for item in lst:
		if (item[0] < 0.000001):
			tempLst.append(item[1])

	return tempLst

def removeExtraFeatures(df,clf):
	names = df.columns.values
	lst = sorted(zip(map(lambda x: round(x, 7), clf.feature_importances_), names), reverse=True)
	lst = redundantFeature(lst)
	df = df.drop(lst,axis = 1)
	return df

def addFileSize(x):
	filename = 'ASM/' + x + '.asm'
	statInfo = os.stat(filename)
	return statInfo.st_size

df = pd.read_csv('allFeatures.csv',index_col= False)
df['SizeBytes'] = df.Id.apply(lambda x: addFileSize(x))
df = modTrain(df)

X_all = df.drop(['Id','Id1'], axis=1)
y_all = df[['Prediction1','Prediction2','Prediction3','Prediction4','Prediction5','Prediction6','Prediction7','Prediction8','Prediction9']]
X_all = X_all.apply(lambda x: x/x.max(), axis=0)

num_test = 0.3
X_train, X_test, y_train, y_test = train_test_split(X_all, y_all, test_size=num_test, random_state=243)

clf = RandomForestClassifier()
clf.fit(X_train, y_train)


predictions = clf.predict(X_test)

print predictions

print(accuracy_score(y_test, predictions))

df = removeExtraFeatures(df,clf)
df.to_csv('finalFeatures.csv', index = False)