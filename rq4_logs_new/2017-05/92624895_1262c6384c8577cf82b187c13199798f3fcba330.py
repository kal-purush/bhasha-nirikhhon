
''' Importing python packages '''
import scipy.io as sio
import matplotlib.pyplot as plt
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import tree

''' Loading Data '''
data = sio.loadmat( 'ionosphere.mat' )

''' Organizing Data '''
X = data[ 'X' ].reshape( [351,34] )
Y = data[ 'Y' ].reshape( [-1,1] )

print "Starting Assignment\n"

'''-------------------------------------- fixNan() -----------------------------------------'''
'''
Function Name: fixNan()
Function Prototype: def fixNan( X )
Description: this function replaces any NaN data values with zeros. If there are no NaN values
	then the original data set is returned
Parameters: 
	X - arg1 -- data to fix
Return Value: modified data with NaN values replaced wth zeros or original data set
'''
def fixNan( X ):
	print "Cleaning Data"
	X_shape = X.shape
	newX = np.zeros( X_shape )

	for i in range( X_shape[0] ):
		for j in range( X_shape[1] ):
			if np.isnan( X[i,j] ):
				continue
			else:
				newX[i,j] = X[i,j]
	
	return newX

'''---------------------------------- convertLabels() --------------------------------------'''
'''
Function Name: convertLabels()
Function Prototype: def convertLabels( Y, labels )
Description: this fuction convert all the categorical labels into numerical values of binary
	class. If the categorical label is labeled as 'labels' then its set to -1, if not
	then its set to +1.
Parameters:
	Y - arg1 -- dataset containing all the labels for the training and testing set
	labels - arg2 -- categorical label that needs to be taken into account
Return Value: dataset with the modified label values
'''
def convertLabels( Y, label ):
	Y_shape = Y.shape
	newLabels = np.zeros( Y_shape )
	
	for i in range( Y_shape[0] ):
		if Y[i] == label:
			newLabels[i] = -1
		else:
			newLabels[i] = 1

	newLabels = np.ravel( newLabels )
	return newLabels

'''------------------------------------- splitData() ---------------------------------------'''
'''
Function Name: splitData()
Function Prototype: def splitData( X, Y, testSize )
Description: this function splits the input data into testing and training sets
Parameters:
	X - arg1 -- data containing all the features
	Y - arg2 -- data containing all the labels
	testSize - arg3 -- size of the testing data, in the range of (0,1) exclusive
Return Value: this function will return the following four datasets in this order
	Xtrain -- new training set containing all the features
	Xtest -- new testing set containing all the features
	Ytrain -- new training set containin all the labels of the corresponding training set
	Ytest -- new testing set containing all the labels of the corresponding testing set
'''
def splitData( X, Y, testSize ):
	Xtrain, Xtest, Ytrain, Ytest = train_test_split( X, Y, test_size = testSize, random_state = 42 )
	print "Splitting Data"
	print "Train Size = ", (1 - testSize)
	print "Test Size = ", testSize
	return Xtrain, Xtest, Ytrain, Ytest

'''---------------------------------- decision_tree() ------------------------------------------'''
def decision_tree( Xtrain, Ytrain, Xtest, depth = None ):
	clf = tree.DecisionTreeClassifier( max_depth = depth )
	clf = clf.fit( Xtrain, Ytrain )
	predict = clf.predict( Xtest )
	return predict	

'''--------------------------------- err_decisionTree() ----------------------------------------'''
def err_decisionTree( predict, Ytest ):
	misClassified = 0
	result = predict + Ytest
	numOfTestData = float( result.size )
	for i in result:
		if i == 0:
			misClassified = misClassified + 1
	error = misClassified / numOfTestData
	return error
	
'''"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""'''
'''"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""'''
X = fixNan( X )
Y = convertLabels( Y, 'b' )

'''"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""'''
'''"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""'''
'''------------------------------ Decision Tree Classification ---------------------------------'''
Xtrain, Xtest, Ytrain, Ytest = splitData( X, Y, 0.2 )
predict = decision_tree( Xtrain, Ytrain, Xtest )
err = err_decisionTree( predict, Ytest )