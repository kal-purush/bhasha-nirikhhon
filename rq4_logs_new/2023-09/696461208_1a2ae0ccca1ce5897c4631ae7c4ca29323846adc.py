# -*- coding: utf-8 -*-
"""
Created on Sun Sep 24 15:25:20 2023

@author: azamb
"""

import pandas as pd

#Importing data
df=pd.read_csv('ca1-dataset.csv')
df=df.sort_values(by='namea')

#Replacing Target by 0s and 1s
df['OffTask'] = df['OffTask'].replace({'Y': 1, 'N': 0})

#Distribution of each Class (OnTask vs OffTask)
OffTaskSamples=sum(df['OffTask'])/len(df)
print("Off-Task: " + str(OffTaskSamples*100)+ "%")
print("On-Task: " + str(100-OffTaskSamples*100)+ "%")

#Getting number of unique students
Students=df['namea'].unique()
print('Students: '+str(len(Students)))

#Getting students that were OffTask at some point
OffTaskStudents=df[df['OffTask']==1]['namea'].unique()
print('Off Task Students: '+str(len(OffTaskStudents)))

#Droping columns without variance
columns_to_drop=['Avghelp','Avgchoice','Avgstring','Avgnumber','Avgpoint','Avghelppct-up',
                 'Avgrecent8help','AvgasymptoteA-up','AvgasymptoteB-up']
df = df.drop(columns=columns_to_drop)

#Verifying correlations between features

from scipy.stats import spearmanr
import seaborn as sns
import matplotlib.pyplot as plt

# Calculate the correlation matrix
correlation_matrix = df.corr()

# Create a heatmap of the correlation matrix
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Correlation Heatmap")
plt.show()

#Droping Avgnotright because is providing the same information than Avgright
columns_to_drop2=['Avgnotright']  #Other options to drop are Avgprev5Count-up, Avgtimelast5SDnormed and Avgtime
df = df.drop(columns=columns_to_drop2)

#Create a column indicating if the student was offtask at any moment (for stratified k fold)
df['AnyOffTask']=0
df.loc[df['namea'].isin(OffTaskStudents), 'AnyOffTask'] = 1

#Groups for student level CV
groups=df['namea']

# Create a 10-fold student level cross-validation object, stratified by the target
from sklearn.model_selection import cross_val_score, KFold
from sklearn.model_selection import StratifiedGroupKFold
import numpy as np

kf = StratifiedGroupKFold(n_splits=10)
#Vector for stratified kFold (Not the target!!!)
yKFold = np.array(df['AnyOffTask'])
#Target
y=np.array(df['OffTask'])
#Features
X=np.array(df.drop(columns=['Unique-id','namea','OffTask','AnyOffTask'], axis=1))


#Cross Validation testing several models
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import make_scorer, cohen_kappa_score
from sklearn.naive_bayes import GaussianNB
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.metrics import cohen_kappa_score
from sklearn.metrics import precision_score, recall_score
from sklearn.svm import SVC

#Lists to save results
Kappa_scores=[]
AUC_scores=[]
Precision = []
Recall = []

#Flag to activate SMOTE
from imblearn.over_sampling import SMOTE
smote = SMOTE(sampling_strategy=0.25,random_state=42)
DoSMOTE=0

#Flag to balance based on class weights
ClassWeights=0

#Cross Validation
for train_index, test_index in kf.split(X, yKFold, groups):
    
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]
    
    if DoSMOTE==1:
        X_train,y_train=smote.fit_resample(X_train,y_train)
    
    if ClassWeights==1:
        # Compute class weights
        class_counts = np.bincount(y)
        total_samples = len(y)
        class_weights = total_samples / (len(class_counts) * class_counts)
        clf = xgb.XGBClassifier(learning_rate=0.5, n_estimators=200, random_state=5,scale_pos_weight= class_weights[1])
        #clf = LogisticRegression(max_iter=10000, class_weight='balanced')
        #clf = SVC(kernel='rbf', random_state=42, class_weight='balanced', probability=True)
        #clf = RandomForestClassifier(n_estimators=200, class_weight='balanced', random_state=5)
        #clf = DecisionTreeClassifier(class_weight='balanced')
        #clf = GaussianNB(class_weight='balanced')
    
    else:
        #clf = DecisionTreeClassifier()
        #clf = GaussianNB()
        clf = xgb.XGBClassifier(learning_rate=0.5, n_estimators=200, random_state=5)
        #clf = RandomForestClassifier(n_estimators=200, random_state=5)
        #clf = LogisticRegression(max_iter=10000)
        #clf = SVC(kernel='rbf', random_state=42, probability=True)

    clf.fit(X_train, y_train)
    y_trainprob=clf.predict_proba(X_train)[:,1]
    
    #Prediction (Confidence)
    y_pred = clf.predict_proba(X_test)[:,1]
    #Binary Prediction
    y_pred2 = clf.predict(X_test)
    
    # Calculate metrics
    AUC = roc_auc_score(y_test, y_pred)
    AUC_scores.append(AUC)
    kappa = cohen_kappa_score(y_test, y_pred2)
    Kappa_scores.append(kappa)
    precision = precision_score(y_test, y_pred2)
    Precision.append(precision)
    recall = recall_score(y_test, y_pred2)
    Recall.append(recall)

# Print the AUC score for each fold
for fold, score in enumerate(AUC_scores, start=1):
    print(f"Fold {fold}: AUC {score:.4f}")

# Calculate and print the mean AUC score across all folds
mean_AUC = np.mean(AUC_scores)
std_AUC = np.std(AUC_scores)
print(f"Mean AUCROC: {mean_AUC:.4f}")
print(f"STD AUCROC: {std_AUC:.4f}\n")

# Print the Kappa for each fold
for fold, score in enumerate(Kappa_scores, start=1):
    print(f"Fold {fold}: Kappa {score:.4f}")

# Calculate and print the mean Kappa across all folds
mean_Kappa = np.mean(Kappa_scores)
print(f"Mean Kappa: {mean_Kappa:.4f}")
std_Kappa = np.std(Kappa_scores)
print(f"STD Kappa: {std_Kappa:.4f}\n")

# Print the Precision for each fold
for fold, score in enumerate(Precision, start=1):
    print(f"Fold {fold}: Precision {score:.4f}")

# Calculate and print the mean Precision across all folds
mean_Precision = np.mean(Precision)
print(f"Mean Precision: {mean_Precision:.4f}")
std_Precision = np.std(Precision)
print(f"STD Precision: {std_Precision:.4f}\n")

# Print the Recall for each fold
for fold, score in enumerate(Recall, start=1):
    print(f"Fold {fold}: Recall {score:.4f}")

# Calculate and print the mean Recall across all folds
mean_Recall = np.mean(Recall)
print(f"Mean Recall: {mean_Recall:.4f}")
std_Recall = np.std(Recall)
print(f"STD Recall: {std_Recall:.4f}")

#Saving final model
import joblib

# Training last model with all available data
clf.fit(X, y)

# Save the trained model to a file. Rename the file for each new model you want to save
model_filename = "XGBoostAZ.pkl"
joblib.dump(clf, model_filename)

print("Model saved as", model_filename)

