import pandas as pd
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.grid_search import GridSearchCV, RandomizedSearchCV
from sklearn.metrics import classification_report
from sklearn.svm import SVC
from sklearn.metrics import recall_score
from sklearn.externals import joblib
from sklearn.cross_validation import cross_val_predict
from scipy.stats import randint as sp_randint
from time import time
from operator import itemgetter
import numpy as np
from pandas import DataFrame
import os

workspace_path = "C:/development/repos/git/github/ZED/projekt2/"
if not os.path.exists(workspace_path+"data"):
    os.makedirs(workspace_path+"data")

if not os.path.exists(workspace_path+"predictions"):
    os.makedirs(workspace_path+"predictions")

if not os.path.exists(workspace_path+"classifiers"):
    os.makedirs(workspace_path+"classifiers")

train_data_df = pd.read_csv(workspace_path + "data/training_data.txt", sep=";", na_values=["nan"], keep_default_na=False, low_memory=False)

X_test = pd.read_csv(workspace_path + "data/test_data.txt", sep=",", na_values=["nan"], keep_default_na=False, low_memory=False)

print train_data_df.shape
df_filtered = train_data_df.query('res_name not in ("DA","DC","DT","DU","DG","DI","UNK","UNX","UNL","PR","PD","Y1","EU","N","15P","UQ","PX4","NAN")')
print df_filtered.shape

# df_grouped=df_filtered.groupby(['pdb_code', 'res_namee'])
df_grouped = df_filtered.drop_duplicates(['pdb_code', 'res_name'])
print df_grouped.shape;
df_without_classes = df_grouped.groupby("res_name").filter(lambda x: len(x) >= 5)
print 'Filtered training data dim:', df_without_classes.shape

group_label_df = pd.read_csv(workspace_path + "data/grouped_res_name.txt", sep=",", na_values=["nan"], keep_default_na=False, low_memory=False)
print 'Group label dim:', group_label_df.shape


part_00_cols = df_without_classes.filter(regex='part_00_.*');
rest_cols = df_without_classes.filter(items=[
  'local_volume', 'local_electrons', 'local_mean', 'local_std',
  'local_max', 'local_skewness', 'local_parts',
  'solvent_mask_count', 'void_mask_count', 'modeled_mask_count', 'solvent_ratio'])



X_test = X_test.fillna(0)
X_test_part_00_cols = X_test.filter(regex='part_00_.*');
X_test_rest_cols = X_test.filter(items=[
  'local_volume', 'local_electrons', 'local_mean', 'local_std',
  'local_max', 'local_skewness', 'local_parts',
  'solvent_mask_count', 'void_mask_count', 'modeled_mask_count', 'solvent_ratio'])
X_test =  pd.concat([X_test_part_00_cols,  X_test_rest_cols], axis=1);





filtered_df = pd.concat([rest_cols,  part_00_cols], axis=1);
filtered_df = filtered_df.fillna(0);
X_train = filtered_df
y_train = df_without_classes['res_name']







print "X_train", X_train.shape
print "y_train", y_train.shape

rfc = RandomForestClassifier(n_jobs=8)

param_grid = {
              "n_estimators": [100],
              # "max_features": [3],
              # "min_samples_split": [5],
              # "min_samples_leaf": [2],
              "bootstrap": [False],
              "criterion": ["gini"]
}


clf = GridSearchCV(estimator=rfc, param_grid=param_grid, cv=5,
scoring='recall_macro')
clf.fit(X_train, y_train)
joblib.dump(clf.best_estimator_, workspace_path + 'classifiers/classifier1.pkl')

print("Best parameters set found on development set:")
print()
print(clf.best_estimator_)
print()
print "Best score:", clf.best_score_
y_pred =clf.best_estimator_.predict_proba(X_test)

y_pred_df = DataFrame(data=y_pred, columns=clf.best_estimator_.classes_)
y_pred_df.to_csv(workspace_path + "predictions/classifier1_predicts_probability.txt", sep=",")


y_pred =clf.best_estimator_.predict(X_test)

y_pred_df = DataFrame(data=y_pred)
y_pred_df.to_csv(workspace_path + "predictions/classifier1_predicts.txt", sep=",")


y_pred = cross_val_predict(clf.best_estimator_, X_train, y_train, cv=5)
print recall_score(y_train, y_pred, average='macro')












filtered_df = pd.concat([rest_cols,  part_00_cols], axis=1);
filtered_df = filtered_df.fillna(0)
X_train = filtered_df
y_train = group_label_df['res_name_group']


print "X_train", X_train.shape
print "y_train", y_train.shape

rfc = RandomForestClassifier(n_jobs=8 )


param_grid = {
              "n_estimators": [50],
              # "max_depth": [None],
              # "max_features": [3],
              # "min_samples_split": [5],
              # "min_samples_leaf": [2],
              "bootstrap": [False],
              "criterion": ["gini"]
}



clf = GridSearchCV(estimator=rfc, param_grid=param_grid, cv=5,
scoring='recall_macro')
clf.fit(X_train, y_train)
joblib.dump(clf.best_estimator_, workspace_path + 'classifiers/classifier2.pkl')

print("Best parameters set found on development set:")
print()
print(clf.best_estimator_)
print()
print "Best score:", clf.best_score_

y_pred =clf.best_estimator_.predict_proba(X_test)

y_pred_df = DataFrame(data=y_pred, columns=clf.best_estimator_.classes_)
y_pred_df.to_csv(workspace_path + "predictions/classifier2_predicts_probability.txt", sep=",")


y_pred =clf.best_estimator_.predict(X_test)

y_pred_df = DataFrame(data=y_pred)
y_pred_df.to_csv(workspace_path + "predictions/classifier2_predicts.txt", sep=",")


y_pred = cross_val_predict(clf.best_estimator_, X_train, y_train, cv=5)
print recall_score(y_train, y_pred, average='macro')




