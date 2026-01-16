import pandas as pd
import numpy as np
from sklearn import model_selection
from sklearn import metrics
import matplotlib.pyplot as plt
from sklearn.neighbors import KNeighborsClassifier

df = pd.read_csv(r'https://raw.githubusercontent.com/TeamLab/machine_learning_from_scratch_with_python/master/code/ch12/titanic/train.csv',engine='python',encoding='cp949')
print(df.shape)
print( pd.unique(df['Pclass'])) #hot 

df=df.drop(columns='PassengerId')
df=df.drop(columns='Name')
print(df)
print( pd.unique(df['Sex'])) # male:0 female:1
#https://statkclee.github.io/ml/ml-modeling-titanic.html


print(pd.unique(df['SibSp']))
df=df.drop(columns='Ticket')
print(df)

df=df.drop(columns='Cabin')
print(df)

print(pd.unique(df['Embarked']))

print(df.head())

#df=df.dropna()
#df=df.dropna(how='all')
#df=df.dropna(thresh=3)
df['Age']=df['Age'].fillna(df['Age'].mean())
df['Embarked']=df['Embarked'].fillna(df['Embarked'].mode()[0])
print(df.isnull())
print(df.isnull().sum())


df['Sex']=df['Sex'].replace(['female','male'],[0,1])
print(df.head())
df['Pclass']=df['Pclass'].replace([1,2,3],['a','b','c'])
print(df.head())
df=pd.get_dummies(df)
print(df.head())

x_data=df.iloc[:,1:]
x_data=x_data.values
print(x_data[:5])
y_data=df['Survived']
y_data=y_data.values
print(y_data[:5])

np.save('titanic_x_data.npy', x_data)
np.save('titanic_y_data.npy', y_data)