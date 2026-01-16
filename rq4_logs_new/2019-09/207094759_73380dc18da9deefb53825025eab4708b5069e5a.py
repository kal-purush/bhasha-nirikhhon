import numpy as np 
import pandas as pd 
from sklearn import linear_model


data = {'layout' : [2, 1, 3, 4, 5], 
        'land_area' : [40, 20, 30, 55, 65],
        'building_area' : [50, 20, 35, 60, 70],
        'age_of_a_building' : [40, 50, 30,20,10],
        'adoress' : [7, 3, 8, 4, 1],
        'price' : [48000, 29000, 33000,55000,67000]}
         
df = pd.DataFrame(data)

Y_train = df['price']
X_train = df.drop("price", axis=1)

lm_model = linear_model.LinearRegression()
lm_model.fit(X_train,Y_train)

print("切片 =",lm_model.intercept_)
print("回帰係数 =",lm_model.coef_)