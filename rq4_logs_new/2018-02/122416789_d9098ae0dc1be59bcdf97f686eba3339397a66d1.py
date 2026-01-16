import pandas as pd
import numpy as np
import sys
import numbers

'''find Euclidean distance and return list'''
def EuclideanDistance(test, train):
    sumSq = 0.0
    '''test is a row from testing set and train is a row from training set'''
    for i in range(len(test)-1):
        if (isinstance(test[i],numbers.Number)):
            sumSq += (test[i] - train[i]) ** 2
        elif(test[i] == train[i]):
            sumSq += 0
        else:
            sumSq += 1
    result = sumSq ** 0.5
    return result

'''Take input of k and file names as command line arguments'''
k = int(sys.argv[1])
train = sys.argv[2]
test = sys.argv[3]

''' Read in the files into train and test data frames '''
train_df = pd.read_csv(train, header=None)
test_df = pd.read_csv(test, header=None)

''' Remove the class attribute column from the train and test dataframes and store it in a different variable '''
train_df_withoutclass = train_df.loc[:, train_df.columns != 15]
test_df_withoutclass = test_df.loc[:, test_df.columns != 15]

new_column_withpredictions = []
for i in range(len(test_df)):
    Distance_df = []
    for j in range(len(train_df)):
        Distance_df.append(EuclideanDistance(test_df.loc[i,test_df.columns != 15],train_df.loc[j,train_df.columns != 15]))
    Distance_df = np.array(Distance_df)
    nearest_neighbors = Distance_df.argsort()[:k]
    NN_labels = train_df.loc[nearest_neighbors,train_df.columns[-1]]
    NN_labels = NN_labels.tolist()
    NN_label = max(set(NN_labels), key= NN_labels.count)
    new_column_withpredictions.append(NN_label)
test_df.loc[:,test_df.columns[-1]+1] = new_column_withpredictions
test_df.to_csv( test +".output",header=None, index=None, sep=',', mode='w')
