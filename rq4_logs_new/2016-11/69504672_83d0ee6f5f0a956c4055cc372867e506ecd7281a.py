from sklearn import datasets
from sklearn import linear_model
import matplotlib.pyplot as plt
import numpy as np
from numpy import genfromtxt
import csv

my_data=genfromtxt('training.csv',delimiter=',')
print(my_data.shape)
X=my_data[1:1001,1:2]
print(X.shape)
for i in range(1000):
    for j in range(1):
        if(X[i][j]==np.NaN or X[i][j]==np.Infinity):
            X[i][j]=0.0
y=my_data[1:1001,3599:3600]
for k in range(1000):
    for l in range(1):
        if(y[k][l]==np.NaN or y[k][l]==np.Infinity):
            y[k][l]=0.0
print(y.shape)

x_test=my_data[1002:1503,1:2]
print(x_test.shape)
y_test=my_data[1002:1503,3599:3600]
print(y_test.shape)

"""with open('training.csv','rb') as f:
    reader=csv.reader(f)
    for row in reader:
        print(row)"""
    
lr=linear_model.LinearRegression()
lr.fit(X,y)
print("COefficients",lr.coef_)
list_coef=lr.coef_.flatten()
sum=0
"""for i in range(len(list_coef)-1):
    sum+=list_coef[i]*X[1][i]
    print("sum",sum,"actual value",X[])"""
    


plt.scatter(X,y,color='black')
plt.plot(X,lr.predict(X),color='blue',linewidth=3)
plt.xlabel('data')
plt.ylabel('target')
plt.title('Linear Regression')
plt.xticks(())
plt.yticks(())
plt.show()
'''subtract=y_test.flatten()-lr.predict(x_test)
error=(((np.abs(subtract))**2).sum())/156
print(error)'''
sum_mean = 0
for i in range(len(lr.predict(x_test))):
    sum_mean += (lr.predict(x_test[i])-y_test[i])**2  
    #sum_error=np.sqrt(sum_mean/predict_outcome.shape[0])
print(sum_mean)
print((sum_mean/lr.predict(x_test).shape[0])**0.5)