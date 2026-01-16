import pandas as pd
import quandl, math
import numpy as np
from sklearn import preprocessing, cross_validation, svm
from sklearn.linear_model import LinearRegression

quandl.ApiConfig.api_key = 'YOUR-API-KEY'
df = quandl.get_table('WIKI/PRICES')


df = df[['adj_open','adj_high','adj_low','adj_close','adj_volume']]
df['HL_PCT'] = (df['adj_high'] - df['adj_close']) / df['adj_close'] * 100
df['PCT_Change'] = (df['adj_close'] - df['adj_open']) / df['adj_open'] * 100

df = df[['adj_close','HL_PCT','PCT_Change','adj_volume']]

forecast_col = 'adj_close'
df.fillna(-9999, inplace=True)

forecast_out = int(math.ceil(0.01*len(df)))
print(forecast_out + ' days in advance')

df['label'] = df[forecast_col].shift(-forecast_out)
df.dropna(inplace=True)

X = np.array(df.drop(['label'],1))
y = np.array(df['label'])
X = preprocessing.scale(X)
y = np.array(df['label'])

X_train, X_test, y_train, y_train = cross_validation.train_test_split(X, y, test_size = 0.2)

#using the linear regression
clf = LinearRegression()
clf.fit(x_train, y_train)
accuracy = clf.score(X_test, y_test)
print(accuracy)

#using the support vector machine
clf = svm.SVR(kernel='poly')
clf.fit(x_train, y_train)
accuracy = clf.score(X_test, y_test)
print(accuracy)


# print(df.head())