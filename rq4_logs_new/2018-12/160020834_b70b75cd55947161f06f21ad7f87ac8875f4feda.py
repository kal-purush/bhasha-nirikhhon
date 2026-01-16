import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import mean_squared_error
from sklearn import linear_model,svm,tree
import datetime as dt

path = 'E:\ML\BigStock\Stocks\wmt.us.txt'
name = ['Date','Open','High','Low','Close','Volume','OpenInt']
data = pd.read_csv(path,names=name)
data = data.drop(data.index[0])

data['Date'] = pd.to_datetime(data['Date'])
data['Date'] = data['Date'].map(dt.datetime.toordinal)

x= np.array(data[['Date']])
y = np.array(data['Close'])

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2)

#Linear Rregression

lrmod = linear_model.LinearRegression()
lrmod.fit(x_train,y_train)
y_pred = lrmod.predict(x_test)
print("Mean squared error lr: %f" % mean_squared_error(y_test, y_pred))


#Logistic Rregression

clf = LogisticRegression(random_state=0, solver='lbfgs', multi_class='multinomial')
clf.fit(x_train, y_train)
y_pred = clf.predict(x_test)
print("Mean squared error lgr: %f" % mean_squared_error(y_test, y_pred))


#kNN

neigh = KNeighborsClassifier(n_neighbors=3)
neigh.fit(x_train, y_train)
y_pred = neigh.predict(x_test)
print("Mean squared error knn: %f" % mean_squared_error(y_test, y_pred))

#Decision Tree

clf = tree.DecisionTreeClassifier()
clf.fit(x_train, y_train)
print("Mean squared error dt: %f" % mean_squared_error(y_test, y_pred))

#SVM
clf1 = svm.SVC(gamma='scale')
clf1.fit(x_train, y_train)
y_pred = clf1.predict(x_test)
print("Mean squared error svm: %f" % mean_squared_error(y_test, y_pred))
