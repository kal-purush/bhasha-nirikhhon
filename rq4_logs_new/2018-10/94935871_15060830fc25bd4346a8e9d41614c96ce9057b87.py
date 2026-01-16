# Logistic Regression Titanic Problem

# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

# Importing the dataset
train_dataset = pd.read_csv('train.csv')
test_dataset = pd.read_csv('test.csv')

X_train = train_dataset.iloc[:, [4,5]].values
y_train = train_dataset.iloc[:, 1].values
X_test = test_dataset.iloc[:, [4,5]].values
y_test = test_dataset.iloc[:, 1].values

from sklearn.preprocessing import Imputer
imputer_train = Imputer(missing_values = 'NaN', strategy="mean", axis=0)
imputer_train = imputer_train.fit(X_train[:, 1:2])
X_train[:, 1:2] = imputer_train.transform(X_train[:, 1:2])

from sklearn.preprocessing import LabelEncoder
labelencoder_train = LabelEncoder()
X_train[:, 0] = labelencoder_train.fit_transform(X_train[:, 0])