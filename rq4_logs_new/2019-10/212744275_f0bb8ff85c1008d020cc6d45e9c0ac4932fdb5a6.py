import stats_clf                        # stats_clf.py must be in the same folder
from scipy.io import loadmat
import numpy as np
from math import log,e,exp,pi
from datetime import datetime
from util import print_result
# import matplotlib.pyplot as plt



print('Starting script at ',datetime.now())
begin = datetime.now()

### Load data and preprocess ###
data = loadmat('spamData.mat')          # Assume data file is located in the same folder

xTrain = data['Xtrain']
xTest = data['Xtest']
yTrain = data['ytrain']
yTest = data['ytest']

xTrain_log = [log(val+0.1) for idx,val in np.ndenumerate(xTrain)]
xTest_log = [log(val+0.1) for idx,val in np.ndenumerate(xTest)]

xTrain_log = np.asarray(xTrain_log)
xTrain_log = xTrain_log.reshape((xTrain.shape[0],xTrain.shape[1]))
xTest_log = np.asarray(xTest_log)
xTest_log = xTest_log.reshape((xTest.shape[0],xTest.shape[1]))

xTrain_bi = np.zeros((xTrain.shape[0],xTrain.shape[1]))
xTest_bi = np.zeros((xTest.shape[0],xTest.shape[1]))

xTrain_bi[xTrain > 0] = 1
xTest_bi[xTest > 0] = 1

### Training and Classification ###

# Beta-Bionomial Naive Bayes Classifier
print('\nBeta-Bionomial Classifier \n')
BiClf = stats_clf.BiBetaNB(0)

train_Bi = []
eval_Bi = []
a_val = [i*0.5 for i in range(201)]             # Values of a to be fitted to beta-bi
for a in a_val:
    BiClf.set_params(a)
    train_Bi.append(BiClf.fit(xTrain_bi,yTrain))
    eval_Bi.append(BiClf.evaluate(xTest_bi,yTest))

print_result(train_Bi,eval_Bi,a_val,1,10,100)

#Gaussian Naive Bayes Classifier
print('\nGaussian Classifier\n')
GaClf = stats_clf.GaussianNB()

train_Ga = GaClf.fit(xTrain_log,yTrain)
eval_Ga = GaClf.evaluate(xTest_log,yTest)

print("Training error and accuracy: ",train_Ga,'\n',
"Test error and accuracy: ",eval_Ga)

#Logistic Regression Classifier
print('\nLogistic Regression Classifier\n')
LogClf = stats_clf.BiLogReg(0)

lamb_val = [i for i in range(1,10)]+[i for i in range(10,101,5)]     # Values of parameter fitted to logistic and knn
train_Log = []
eval_Log = []                                   
for lamb in lamb_val:                           
    LogClf.set_params(lamb)
    train_Log.append(LogClf.fit(xTrain_log,yTrain))
    eval_Log.append(LogClf.evaluate(xTest_log,yTest))

print_result(train_Log,eval_Log,lamb_val,1,10,100)

# K-Nearest Neighbors Classifier
print('\nK-Nearest Neighbors Classifier\n')
knn = stats_clf.KNNeighbors(1)

train_knn = []
eval_knn = []
for k in lamb_val:
    knn.set_params(k)
    train_knn.append(knn.fit(xTrain_log,yTrain))
    eval_knn.append(knn.evaluate(xTest_log,yTest))

print_result(train_knn,eval_knn,lamb_val,1,10,100)

print('Finishes classification at ',datetime.now())
print('Total runtime: ',datetime.now() - begin)

### Plotting code ###

# Uncomment the below and import matplotlib.pyplot to run. 
# Will save images to .png files to the same current folder #

# f1 = plt.figure(dpi=100)
# ax1 = f1.subplots()
# ax1.plot(lamb_val,np.asarray(train_knn)[:,0])
# ax1.plot(lamb_val,np.asarray(eval_knn)[:,0])
# ax1.legend(['Training error','Evaluate error'])

# f2 = plt.figure(dpi=100)
# ax1 = f2.subplots()
# ax1.plot(lamb_val,np.asarray(train_Log)[:,0])
# ax1.plot(lamb_val,np.asarray(eval_Log)[:,0])
# ax1.legend(['Training error','Evaluate error'])

# f3 = plt.figure(dpi=100)
# ax1 = f3.subplots()
# ax1.plot(a_val,np.asarray(train_Bi)[:,0])
# ax1.plot(a_val,np.asarray(eval_Bi)[:,0])
# ax1.legend(['Training error','Evaluate error'])

# f1.savefig('K-Nearest Neighbors Error.png')
# f2.savefig('Logistics Regression Error.png')
# f3.savefig('Beta-Bionmial Error.png')