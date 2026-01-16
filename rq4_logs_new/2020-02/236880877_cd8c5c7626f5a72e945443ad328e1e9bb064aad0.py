# %% [markdown]
# # Titanic Survival with Python
#         
#    **Objective is to develop a classification model to predict, if a passenger survives or perishes. Let's begin our understanding of the dataset followed by widely used classification algorithm.**
# 
# ## Importing Libraries
# 
#    Let's import libraries to get started!

# %% [code]
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC,NuSVC
from sklearn.ensemble import AdaBoostClassifier,RandomForestClassifier,GradientBoostingClassifier,VotingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis, QuadraticDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB, BernoulliNB, MultinomialNB
from sklearn.neighbors import KNeighborsClassifier,RadiusNeighborsClassifier
from sklearn.decomposition import PCA
from sklearn.decomposition import FastICA
from keras.models import Sequential
from keras.layers import Dense
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedKFold,StratifiedShuffleSplit
from sklearn.model_selection import GridSearchCV
from sklearn import metrics as met
from sklearn.metrics import classification_report
import warnings
warnings.filterwarnings('ignore')
%matplotlib inline

# %% [markdown]
# ## Dataset - Train and Test Dataset. Find columns in testing and training set and Number of records available in each set.
# 
# Import the train and test dataset and verify the columns available.

# %% [code]
training_set = pd.read_csv('../input/train.csv')
testing_set = pd.read_csv('../input/test.csv')
pID = testing_set['PassengerId']

# %% [code]
print(training_set.shape)

# %% [code]
print(testing_set.shape)

# %% [code]
print(training_set.columns)

# %% [code]
print(testing_set.columns)

# %% [markdown]
#   # Data Exploration :
#   
#   ### 'Survived' column is the target variable which needs to be predicted and is not present in testing set.
#   

# %% [code]
training_set.head()

# %% [code]
training_set.describe()

# %% [markdown]
# **By Using .info() command, we can notice that "Age" and "Cabin" column have missing values.**
# 
# **Roughly 20 percent values in Age column is missing. We can make a reasonable impution on Age column. But Cabin column, we are missing approx 80% values. We'll drop the cabin column.**

# %% [markdown]
# ## Plot the Data

# %% [markdown]
# **Performing basic visulization with the help of Seaborn.**

# %% [code]
sns.heatmap(training_set.isnull(),yticklabels=False,cbar=False,cmap='Dark2')

# %% [code]
sns.heatmap(testing_set.isnull(),yticklabels=False,cbar=False,cmap='Dark2')

# %% [code]
sns.set_style('whitegrid')
sns.countplot(x='Survived',data=training_set,palette='RdBu_r')

# %% [markdown]
# **The plot between Gender and Target variable, clearly suggest that more men have suffered the fate of Jack from Titanic.**

# %% [code]
sns.set_style('whitegrid')
sns.countplot(x='Survived',hue='Sex',data=training_set,palette='RdBu_r')
plt.title("Gender vs Survived")
plt.legend(loc = 'top left')

# %% [code]
sns.set_style('whitegrid')
sns.countplot(x='Survived',hue='Pclass',data=training_set,palette='rainbow')

# %% [code]
sns.set_style('whitegrid')
sns.countplot(x='Survived',hue='Embarked',data=training_set,palette='Dark2')
plt.legend(loc = 'top left',bbox_to_anchor=(1.2, 1.2))

# %% [code]
sns.distplot(training_set['Age'].dropna(),kde=False,color='darkred',bins=30)

# %% [code]
sns.countplot(x='SibSp',data=training_set)

# %% [code]
training_set['Fare'].hist(color='green',bins=40,figsize=(8,4))

# %% [markdown]
# ## Data Cleaning
# 
# Fill in missing age data instead of just dropping the missing age data rows. One way to do this is by filling in the mean age of all the passengers (imputation). However, we can be smarter about this and check the average age by passenger class.

# %% [code]
print(training_set.isnull().sum(),"\n")
print(testing_set.isnull().sum())

# %% [markdown]
# **Feature Engineering - https://www.kaggle.com/ldfreeman3/a-data-science-framework-to-achieve-99-accuracy**

# %% [code]
for dataset in [training_set,testing_set]:    
    #complete missing age with median
    dataset['Age'].fillna(dataset['Age'].median(), inplace = True)

    #complete embarked with mode
    dataset['Embarked'].fillna(dataset['Embarked'].mode()[0], inplace = True)

    #complete missing fare with median
    dataset['Fare'].fillna(dataset['Fare'].median(), inplace = True)
    
#delete the cabin feature/column and others previously stated to exclude in train dataset
drop_column = ['PassengerId','Cabin', 'Ticket']
training_set.drop(drop_column, axis=1, inplace = True)
testing_set.drop(drop_column, axis=1, inplace = True)
print(training_set.isnull().sum())
print("-"*10)
print(testing_set.isnull().sum())

# %% [code]
###CREATE: Feature Engineering for train and test/validation dataset
for dataset in [training_set,testing_set]:    
    #Discrete variables
    dataset['FamilySize'] = dataset ['SibSp'] + dataset['Parch'] + 1

    dataset['IsAlone'] = 1 #initialize to yes/1 is alone
    dataset['IsAlone'].loc[dataset['FamilySize'] > 1] = 0 # now update to no/0 if family size is greater than 1

    #quick and dirty code split title from name: http://www.pythonforbeginners.com/dictionary/python-split
    #dataset['Title'] = dataset['Name'].str.split(", ", expand=True)[1].str.split(".", expand=True)[0]

    #Continuous variable bins; qcut vs cut: https://stackoverflow.com/questions/30211923/what-is-the-difference-between-pandas-qcut-and-pandas-cut
    #Fare Bins/Buckets using qcut or frequency bins: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.qcut.html
    dataset['FareBin'] = pd.qcut(dataset['Fare'], 4)

    #Age Bins/Buckets using cut or value bins: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.cut.html
    dataset['AgeBin'] = pd.cut(dataset['Age'].astype(int), 5)


    
#cleanup rare title names
# #print(data1['Title'].value_counts())
# stat_min = 10 #while small is arbitrary, we'll use the common minimum in statistics: http://nicholasjjackson.com/2012/03/08/sample-size-is-10-a-magic-number/
# title_names = (training_set['Title'].value_counts() < stat_min) #this will create a true false series with title name as index

# #apply and lambda functions are quick and dirty code to find and replace with fewer lines of code: https://community.modeanalytics.com/python/tutorial/pandas-groupby-and-python-lambda-functions/
# training_set['Title'] = training_set['Title'].apply(lambda x: 'Misc' if title_names.loc[x] == True else x)
# print(training_set['Title'].value_counts())
# print("-"*10)


#preview data again
training_set.info()
testing_set.info()
training_set.sample(10)

# %% [code]
training_set[training_set["Name"].str.contains("Master")]

# %% [code]
#CONVERT: convert objects to category using Label Encoder for train and test/validation dataset

#code categorical data
label = LabelEncoder()
for dataset in [training_set,testing_set]:    
    dataset['Sex_Code'] = label.fit_transform(dataset['Sex'])
    dataset['Embarked_Code'] = label.fit_transform(dataset['Embarked'])
    #dataset['Title_Code'] = label.fit_transform(dataset['Title'])
    dataset['AgeBin_Code'] = label.fit_transform(dataset['AgeBin'])
    dataset['FareBin_Code'] = label.fit_transform(dataset['FareBin'])


#define y variable aka target/outcome
Target = ['Survived']

#define x variables for original features aka feature selection
training_set_x = ['Sex','Pclass', 'Embarked','SibSp', 'Parch', 'Age', 'Fare', 'FamilySize', 'IsAlone'] #pretty name/values for charts
training_set_x_calc = ['Sex_Code','Pclass', 'Embarked_Code','SibSp', 'Parch', 'Age', 'Fare'] #coded for algorithm calculation
training_set_xy =  Target + training_set_x
print('Original X Y: ', training_set_xy, '\n')


#define x variables for original w/bin features to remove continuous variables
training_set_x_bin = ['Sex_Code','Pclass', 'Embarked_Code', 'FamilySize', 'AgeBin_Code', 'FareBin_Code']
training_set_xy_bin = Target + training_set_x_bin
print('Bin X Y: ', training_set_xy_bin, '\n')


#define x and y variables for dummy features original
training_set_dummy = pd.get_dummies(training_set[training_set_x],drop_first=True)
training_set_x_dummy = training_set_dummy.columns.tolist()
training_set_xy_dummy = Target + training_set_x_dummy
print('Dummy X Y: ', training_set_xy_dummy, '\n')

training_set_dummy.head()

# %% [code]
y = training_set['Survived']
X = training_set_dummy

# %% [code]
testing_set_dummy = pd.get_dummies(testing_set[training_set_x],drop_first=True)

# %% [code]
ss = MinMaxScaler()
#ss = StandardScaler()
training_set_dummy_ss= ss.fit_transform(training_set_dummy)
testing_set_dummy_ss= ss.fit_transform(testing_set_dummy)

# %% [code]
# pca = PCA(n_components=6)
# X_train_pca = pca.fit_transform(training_set_dummy)
# X_test_pca = pca.transform(testing_set_dummy)

# %% [code]
# transformer = FastICA()
# X_train_ica = transformer.fit_transform(training_set_dummy)

# %% [code]
# X_test_ica = transformer.transform(testing_set_dummy)

# %% [markdown]
# # Building Machine learning Models :

# %% [code]
# Models
classifiers = {'Gradient Boosting Classifier':GradientBoostingClassifier(),'Adaptive Boosting Classifier':AdaBoostClassifier(),'RadiusNN':RadiusNeighborsClassifier(radius=40.0),
               'Linear Discriminant Analysis':LinearDiscriminantAnalysis(), 'GaussianNB': GaussianNB(), 'BerNB': BernoulliNB(), 'KNN': KNeighborsClassifier(),
               'Random Forest Classifier': RandomForestClassifier(min_samples_leaf=10,min_samples_split=20,max_depth=4),'Decision Tree Classifier': DecisionTreeClassifier(),'Logistic Regression':LogisticRegression(), "XGBoost": xgb.XGBClassifier()}

# %% [markdown]
# ## Sampling Data

# %% [markdown]
# ### Train test split

# %% [code]
X_training, X_validating, y_training, y_validating = train_test_split(training_set_dummy, y, test_size=0.20, random_state=11)

# %% [code]
base_accuracy = 0
for Name,classify in classifiers.items():
    classify.fit(X_training,y_training)
    y_predictng = classify.predict(X_validating)
    print('Accuracy Score of '+str(Name) + " : " +str(met.accuracy_score(y_validating,y_predictng)))
    if met.accuracy_score(y_validating,y_predictng) > base_accuracy:
        predictions_test = classify.predict(testing_set_dummy)
        base_accuracy = met.accuracy_score(y_validating,y_predictng)
    else:
        continue

# Generate Submission File 
predicted_test_value = pd.DataFrame({ 'PassengerId': pID,
                        'Survived': predictions_test })
predicted_test_value.to_csv("PredictedTestScore.csv", index=False)

# %% [markdown]
# ### Stratified KFold Sampling

# %% [code]
# skfold = StratifiedKFold(n_splits=2,random_state=42,shuffle=True)
# for Name,classify in classifiers.items():
#     for train_KF, test_KF in skfold.split(X,y):
#         X_train,X_test = X.iloc[train_KF], X.iloc[test_KF]
#         y_train,y_test = y.iloc[train_KF], y.iloc[test_KF]
#         classify.fit(X_train,y_train)
#         y_pred = classify.predict(X_test)
#         print('Accuracy Score of '+str(Name) + " : " +str(met.accuracy_score(y_test,y_pred)))
#         print(classification_report(y_test,y_pred))

# %% [markdown]
# ### Stratified Shuffle Split

# %% [code]
# sss = StratifiedShuffleSplit(n_splits=1,test_size=0.3,random_state=1)
# for Name,classify in classifiers.items():
#     for train_KF, test_KF in sss.split(X,y):
#         X_train,X_test = X.iloc[train_KF], X.iloc[test_KF]
#         y_train,y_test = y.iloc[train_KF], y.iloc[test_KF]
#         classify.fit(X_train,y_train)
#         y_pred = classify.predict(X_test)
#         print('Accuracy Score of '+str(Name) + " : " +str(met.accuracy_score(y_test,y_pred)))
#         print(classification_report(y_test,y_pred))

# %% [markdown]
# ### GridSearchCV

# %% [markdown]
# **GridSearchCV for SVC**

# %% [code]
# param_grid = {'C':[5000],'gamma':[0.0001]}
# gscv = GridSearchCV(SVC(),param_grid)
# gscv.fit(X_training,y_training)
# predictions = gscv.predict(X_validating)
# print(met.accuracy_score(y_validating,predictions))
# print(gscv.best_params_)
# print(gscv.best_score_)

# %% [markdown]
# **GridSearchCV for Gradient Boosting Classifier**

# %% [code]
# param_grid = {'learning_rate':[0.1],"n_estimators":[40],'min_samples_leaf':[15],'min_samples_split':[45],"max_depth":[3],'loss': ['deviance'],"max_features":["auto"]}
# gbccv = GridSearchCV(GradientBoostingClassifier(),param_grid)
# gbccv.fit(X_training,y_training)
# predictions_train = gbccv.predict(X_validating)
# print(met.accuracy_score(y_validating,predictions_train))
# print(gbccv.best_params_)
# print(gbccv.best_score_)

# %% [markdown]
# **GridSearchCV for XGBoost**

# %% [code]
# param_grid = {'learning_rate':[0.1],'gamma':[0.4],"n_estimator":[10],"max_depth":[3]}
# xgbcv = GridSearchCV(xgb.XGBClassifier(),param_grid)
# xgbcv.fit(X_training,y_training)
# predictions_train = xgbcv.predict(X_validating)
# print(met.accuracy_score(y_validating,predictions_train))
# print(xgbcv.best_params_)
# print(xgbcv.best_score_)

# %% [markdown]
# **GridSearchCV for Random Forest Classifier**

# %% [code]
# param_grid = {'min_samples_leaf':[10],'min_samples_split':[20],"max_depth":[5]}
# xgbcv = GridSearchCV(RandomForestClassifier(),param_grid)
# xgbcv.fit(X_training,y_training)
# predictions_train = xgbcv.predict(X_validating)
# print(met.accuracy_score(y_validating,predictions_train))
# print(xgbcv.best_params_)
# print(xgbcv.best_score_)

# %% [code]
# QDA = QuadraticDiscriminantAnalysis()
# QDA.fit(X_training,y_training)

# %% [code]
clf1 = GradientBoostingClassifier()
clf2 = RandomForestClassifier()
clf3 = LinearDiscriminantAnalysis()
clf4 = LogisticRegression()
clf5 = xgb.XGBClassifier()
exTreeClf = VotingClassifier(estimators=[('svc', clf1), ('rfc', clf2), ('gbc', clf3),('lr',clf4),('lda',clf5)], voting='hard')
exTreeClf.fit(X_training,y_training)
y_pred = exTreeClf.predict(X_validating)
print(met.accuracy_score(y_validating,y_pred))

# %% [code]

predictions_test = exTreeClf.predict(testing_set_dummy)
predicted_test_value = pd.DataFrame({ 'PassengerId': pID,
                        'Survived': predictions_test })
predicted_test_value.to_csv("PredictedTestScore.csv", index=False)

# %% [markdown]
# **Deep Learning Model**

# %% [code]
# model = Sequential()
# model.add(Dense(32, input_dim=10, activation='relu'))
# model.add(Dense(32, activation='relu'))
# model.add(Dense(16, activation='relu'))
# model.add(Dense(8, activation='relu'))
# model.add(Dense(1, activation='sigmoid'))
# model.compile(loss='binary_crossentropy', optimizer='rmsprop', metrics=['accuracy'])

# %% [code]
#fit the keras model on the dataset
#model.fit(X_training, y_training, epochs=1000, batch_size=50,validation_data=(X_validating,y_validating),verbose=0)

# %% [code]
# predicted_test = []
# for x in model.predict_classes(X_test_ica):
#     predicted_test.append(x[:][0])

# %% [code]
# predicted_test_value = pd.DataFrame({ 'PassengerId': pID,
#                         'Survived': predicted_test })
# predicted_test_value.to_csv("PredictedTestScore.csv", index=False)

# %% [code]
xgboost = LinearDiscriminantAnalysis()
xgboost.fit(training_set_dummy,y)


# %% [code]
test_index_with_80p = list(np.argwhere(xgboost.predict_proba(testing_set_dummy)>0.75)[:,0])

# %% [code]
y_pred_with_80p = pd.Series(list(np.argwhere(xgboost.predict_proba(testing_set_dummy)>0.75)[:,1]))

# %% [code]
for idx in test_index_with_80p:
    training_set_dummy = training_set_dummy.append(testing_set_dummy.iloc[idx],ignore_index=True)

# %% [code]
y = y.append(y_pred_with_80p,ignore_index=True)

# %% [code]
y.shape

# %% [code]
xgboost = LinearDiscriminantAnalysis()
xgboost.fit(training_set_dummy,y)

# %% [code]
predicted_test = []
for x in xgboost.predict(testing_set_dummy):
    predicted_test.append(x)
predicted_test_value = pd.DataFrame({ 'PassengerId': pID,
                        'Survived': predicted_test })
predicted_test_value.to_csv("PredictedTestScore.csv", index=False)

# %% [code]