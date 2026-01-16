#############################################
#Use Scikit learn to try out 3 different types of regression models to predict the price of that stock for a future date
#############################################
import pandas as pd
import datetime
import pandas_datareader.data as web
from pandas import Series, DataFrame
from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
from matplotlib import style
from sklearn import preprocessing,tree
import math
import numpy as np
#%matplotlib inline
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor

from sklearn.linear_model import Ridge
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline

#Getting stoc prices from yahoo for a ticker
start = datetime.datetime(2010, 1, 1)
end = datetime.datetime(2019, 9, 1)

df = web.DataReader("BAC", 'yahoo', start, end)
df.tail()

#Rolling Mean
close_px = df['Adj Close']
mavg = close_px.rolling(window=100).mean()

# Adjusting the size of matplotlib
import matplotlib as mpl
mpl.rc('figure', figsize=(8, 7))
mpl.__version__

# Adjusting the style of matplotlib
#style.use('ggplot')
#close_px.plot(label='BAC')
#mavg.plot(label='mavg')

#Return Deviation — to determine risk and return
rets = close_px / close_px.shift(1) - 1
#rets.plot(label='return')
#plt.legend()

#Analysing your Competitors Stocks
dfcomp = web.DataReader(['BAC', 'C', 'GOOG', 'WFC', 'STI'],'yahoo',start=start,end=end)['Adj Close']

#Correlation Analysis — Does one competitor affect others?
retscomp = dfcomp.pct_change()
corr = retscomp.corr()

#scatter plt
#plt.scatter(retscomp.BAC, retscomp.C)
#plt.xlabel('Returns BAC')
#plt.ylabel('Returns C')

#scatter matrix
#scatter_matrix(retscomp, diagonal='kde', figsize=(10, 10));

#Heat Map

#plt.imshow(corr, cmap='hot', interpolation='none')
#plt.colorbar()
#plt.xticks(range(len(corr)), corr.columns)
#plt.yticks(range(len(corr)), corr.columns);

#Stocks Returns Rate and Risk
plt.scatter(retscomp.mean(), retscomp.std())
plt.xlabel('Expected returns')
plt.ylabel('Risk')
for label, x, y in zip(retscomp.columns, retscomp.mean(), retscomp.std()):
    plt.annotate(
        label, 
        xy = (x, y), xytext = (20, -20),
        textcoords = 'offset points', ha = 'right', va = 'bottom',
        bbox = dict(boxstyle = 'round,pad=0.5', fc = 'yellow', alpha = 0.5),
        arrowprops = dict(arrowstyle = '->', connectionstyle = 'arc3,rad=0'))
        
#plt.show()


#Implementing Feature Engineering
dfreg = df.loc[:,['Adj Close','Volume']]
dfreg['HL_PCT'] = (df['High'] - df['Low']) / df['Close'] * 100.0
dfreg['PCT_change'] = (df['Close'] - df['Open']) / df['Open'] * 100.0

#Pre-processing & Cross Validation
'''
Drop missing value
Separating the label here, we want to predict the AdjClose
Scale the X so that everyone can have the same distribution for linear regression
Finally We want to find Data Series of late X and early X (train) for model generation and evaluation
Separate label and identify it as y
Separation of training and testing of model by cross validation train test split
'''

# Drop missing value
dfreg.fillna(value=-99999, inplace=True)

# We want to separate 1 percent of the data to forecast
forecast_out = int(math.ceil(0.01 * len(dfreg)))

# Separating the label here, we want to predict the AdjClose
forecast_col = 'Adj Close'
dfreg['label'] = dfreg[forecast_col].shift(-forecast_out)
X = np.array(dfreg.drop(['label'], 1))

# Scale the X so that everyone can have the same distribution for linear regression
X = preprocessing.scale(X)

# Finally We want to find Data Series of late X and early X (train) for model generation and evaluation
X_lately = X[-forecast_out:]
X_train = X[:-forecast_out]

# Separate label and identify it as y
y = np.array(dfreg['label'])
y_train = y[:-forecast_out]


#Model Generation

# Linear regression
clfreg = LinearRegression(n_jobs=-1)
clfreg.fit(X_train, y_train)

# Quadratic Regression 2
clfpoly2 = make_pipeline(PolynomialFeatures(2), Ridge())
clfpoly2.fit(X_train, y_train)

# Quadratic Regression 3
clfpoly3 = make_pipeline(PolynomialFeatures(3), Ridge())
clfpoly3.fit(X_train, y_train)

# KNN Regression
clfknn = KNeighborsRegressor(n_neighbors=2)
clfknn.fit(X_train, y_train)

#Evaluation
#confidencereg = clfreg.score(X_test, y_test)
#confidencepoly2 = clfpoly2.score(X_test,y_test)
#confidencepoly3 = clfpoly3.score(X_test,y_test)
#confidenceknn = clfknn.score(X_test, y_test)

#stocks forecast.
forecast_set = clfreg.predict(X_lately)
dfreg['Forecast'] = np.nan

#Plotting the Prediction
last_date = dfreg.iloc[-1].name
last_unix = last_date
next_unix = last_unix + datetime.timedelta(days=1)

for i in forecast_set:
    next_date = next_unix
    next_unix += datetime.timedelta(days=1)
    dfreg.loc[next_date] = [np.nan for _ in range(len(dfreg.columns)-1)]+[i]
dfreg['Adj Close'].tail(500).plot()
dfreg['Forecast'].tail(500).plot()
plt.legend(loc=4)
plt.xlabel('Date')
plt.ylabel('Price')
plt.show()