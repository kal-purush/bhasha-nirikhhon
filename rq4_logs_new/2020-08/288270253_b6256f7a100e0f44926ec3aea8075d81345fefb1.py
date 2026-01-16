# By: Kevin Wang
# Created: Aug 12th, 2020
### This is the Model Building Process


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier, VotingClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV, RandomizedSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score, precision_score, f1_score, recall_score
from sklearn.utils import resample
import xgboost as xgb

#import cleaned and scaled data 
df = pd.read_csv('cleaned_data_removed_outliers2.csv')
df.columns

#Data Balance 
plt.title('Defaulting Accounts')
sns.countplot(df.def_pay)
plt.show()

#One Hot Encoding
df = pd.get_dummies(df, columns=['SEX', 'EDUCATION', 'MARRIAGE','PAY_1', 'PAY_2', 'PAY_3','PAY_4', 'PAY_5', 'PAY_6'], drop_first= True)
df.columns

#Split into X and y
X = df.drop(['def_pay'], axis = 1)
y = df['def_pay']

#Train test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state = 42)
X.shape[0] == y.shape[0]

#Resampling to fix Imbalance
#Fix Imbalance
df_train = X_train.join(y_train)
df_train.sample(10)

#Seperate into different classes
df_majority = df_train[df_train.def_pay == 0]
df_minority = df_train[df_train.def_pay == 1]

print('Default Counts', df_majority.def_pay.count(),'and No Default Counts', df_minority.def_pay.count())

# Upsample minority class
df_minority_upsampled = resample(df_minority,replace=True,  n_samples=df_majority.def_pay.count() ,random_state=587)  
df_upsampled = pd.concat([df_majority, df_minority_upsampled])

X_train = df_upsampled.drop(['def_pay'], axis = 1)
y_train = df_upsampled['def_pay']

#Check Data Balance
plt.title('Defaulting Accounts')
sns.countplot(df_upsampled.def_pay)
plt.show()

####### Alternative #######
# #Downscale Majority Class 
# # Downsample majority class
# df_majority_downsampled = resample(df_majority, replace=False,  n_samples=df_minority.def_pay.count() ,random_state=587)  
# df_downsampled = pd.concat([df_majority_downsampled, df_minority])

# X_train = df_downsampled.drop(['def_pay'], axis=1)
# y_train = df_downsampled['def_pay']

# #Check Data Balance
# plt.title('Defaulting Accounts')
# sns.countplot(df_downsampled.def_pay)
# plt.show()
#******************************

#Model Building (Minimal Tuning)

#Logistic Regression
logistic = LogisticRegression(max_iter= 10000)
logistic.fit(X_train, y_train)

y_pred = logistic.predict(X_test)
acc = accuracy_score(y_test, y_pred)
precision_score(y_test,y_pred)
recall_score(y_test, y_pred)
f1_score(y_test, y_pred)
print("Logistic Regression Accuracy: {:.2f}".format(acc))

#Decision Tree Classifier
classifier = DecisionTreeClassifier(max_depth=10, random_state=14) 
classifier.fit(X_train, y_train)
y_pred = classifier.predict(X_test)
accuracy_score(y_test, y_pred)
precision_score(y_test, y_pred)
recall_score(y_test, y_pred)
f1_score(y_test, y_pred)

#Random Forest Classifier
rf = RandomForestClassifier(random_state = 99)
rf.fit(X_train, y_train)
y_pred = rf.predict(X_test)

acc = accuracy_score(y_test, y_pred)
precision_score(y_test, y_pred)
recall_score(y_test, y_pred)
f1_score(y_test, y_pred)
print("Random Forest Test data accuracy: {:.5f}".format(acc))

#xgboost
xgboost = xgb.XGBClassifier()
xgboost.fit(X_train, y_train)
y_pred = xgboost.predict(X_test)

acc = accuracy_score(y_test, y_pred)
precision_score(y_test, y_pred)
recall_score(y_test, y_pred)
f1_score(y_test, y_pred)
acc

#Hyper Parameter Tuning
#Random Forest
rf = RandomForestClassifier()
rf_grid = {"n_estimators": [100, 200, 400, 600], "max_depth": [10, 20, 30, 40, 50], "max_features": ['auto', 'sqrt'], 'criterion':['gini', 'entropy'], 'min_samples_split': [2, 5, 10]}
#Xgboost Classifier
xgboost = xgb.XGBClassifier(eval_metric = 'error')
xgb_grid = {'learning_rate': [0.01,0.03, 0.05], 'max_depth': [8, 10, 20, 30], 'n_estimators': [200, 400, 600, 800]}
xgb_search = RandomizedSearchCV(xgboost, xgb_grid, random_state= 99, scoring = 'accuracy', n_iter = 100, n_jobs = -1, verbose = 1)
xgb_search.fit(X_train, y_train)
xgb_search.best_params_
#'n_estimators': 200, 'max_depth': 6, 'learning_rate': 0.03


#Model Tuned

#Random Forest Classifier
rf = RandomForestClassifier(n_estimators = 400, max_features = 'sqrt', max_depth= 50, criterion = 'gini', min_samples_split = 2, random_state=99)
rf.fit(X_train, y_train)
y_pred = rf.predict(X_test)

acc = accuracy_score(y_test, y_pred)
precision_score(y_test, y_pred)
recall_score(y_test, y_pred)
f1_score(y_test, y_pred)
print("Random Forest Test data accuracy: {:.5f}".format(acc))

# >> > precision_score(y_test, y_pred)
# 0.6444249341527656
# >> > recall_score(y_test, y_pred)
# 0.43638525564803804
# >> > f1_score(y_test, y_pred)
# 0.5203828429634881
# >> > print("Random Forest Test data accuracy: {:.5f}".format(acc))
# Random Forest Test data accuracy: 0.81950


#xgboost
xgboost = xgb.XGBClassifier(n_estimators=600, max_depth=20, learning_rate=0.05, eval_metric= 'error', random_state=99)
xgboost.fit(X_train, y_train)
y_pred = xgboost.predict(X_test)

acc = accuracy_score(y_test, y_pred)
precision_score(y_test, y_pred)
recall_score(y_test, y_pred)
f1_score(y_test, y_pred)
print("XGB Classifier Test data accuracy: {:.5f}".format(acc))


#Deep Learning
from keras.models import Sequential
from keras.layers import Dense, Activation, Dropout, BatchNormalization
from keras.callbacks import EarlyStopping
from keras.optimizers import Adam


#Model Architecture
model = Sequential()
model.add(Dense(16, input_dim = 66, activation = 'relu'))
model.add(Dropout(0.5))
model.add(Dense(1, activation = 'sigmoid'))


opt = Adam(lr=1e-3)

model.compile(loss='binary_crossentropy', optimizer= opt, metrics=['accuracy'])

batch = 16
epochs = 100
earlystop = EarlyStopping(patience=30, restore_best_weights= True)
callback = [earlystop]

X_train_arr = np.array(X_train)
X_test_arr = np.array(X_test)
y_train_arr = np.array(y_train)
y_test_arr = np.array(y_test)


history = model.fit(X_train_arr,
                    y_train_arr,
                    validation_data=(X_test_arr, y_test_arr),
                    epochs=epochs,
                    batch_size=batch,
                    callbacks= callback,
                    verbose=1)
