# -*- coding: utf-8 -*-
"""
Created on Wed May 22 10:49:26 2019

@author: hp
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

dataset = pd.read_excel('Desktop/ml datas/AirQualityUCI.xlsx')
X = dataset.iloc[:, 2:12].values
y = dataset.iloc[:, 12].values
                
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y)

from sklearn.linear_model import LinearRegression
lin_reg = LinearRegression()
lin_reg.fit(X_train, y_train)

lin_reg.score(X_train, y_train)
lin_reg.score(X_test, y_test)
lin_reg.score(X, y)