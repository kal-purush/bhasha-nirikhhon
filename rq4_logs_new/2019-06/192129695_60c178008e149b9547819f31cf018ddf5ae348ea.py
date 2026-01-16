# multiple linear regression
#Importing the libraries
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from sklearn import linear_model

#Importing the dataset
df = pd.read_csv('homeprices.csv')

#Finding the missing value
import math
med_bedrooms = math.floor(df.bedrooms.median())
df.bedrooms = df.bedrooms.fillna(med_bedrooms)
print(df)

# Fitting Multiple Linear Regression to the Training set
reg = linear_model.LinearRegression()
reg.fit(df[['area','bedrooms','age']],df.price)

#Finding the coefficients
print(reg.coef_)
print(reg.intercept_)

# Predicting the Test set results
#Find price of home with 3000 sqr ft area, 3 bedrooms, 40 year old
reg.predict([[3000, 3, 40]])
112.06244194*3000 + 23388.88007794*3 + -3231.71790863*40 + 221323.00186540384

#Find price of home with 2500 sqr ft area, 4 bedrooms, 5 year old
reg.predict([[2500, 4, 5]])

plt.xlabel('age')
plt.ylabel('prices')

y = reg.predict(df.iloc[:,:-1].values)
x= df.iloc[:,0]

plt.plot(df.iloc[:,2],df.iloc[:,-1],color ='blue')
plt.plot(df.iloc[:,2,] , y,color = 'red')