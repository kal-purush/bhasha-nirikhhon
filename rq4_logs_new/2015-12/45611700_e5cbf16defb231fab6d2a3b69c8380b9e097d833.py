# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 18:38:01 2015

@author: lypan
"""
#%%
from sklearn import preprocessing
from sklearn.cross_validation import StratifiedKFold
from sklearn.decomposition import PCA
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import SGDClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.svm import LinearSVC
import csv
import numpy as np
import pandas as pd
import random
import re
import time
import xgboost as xgb
#%%#################### original data
start_time = time.time()
# read train and test data from json
traindf = pd.read_json("train.json")
testdf = pd.read_json("test.json")


# data preprocessing
# 1. replace '-' with '_'
# 2. encode from unicode to ascii
# 3. transform from uppercase to lowercase
# 4. add a column for the clean string result
traindf['processed_ingredients'] = traindf.apply(lambda x: [re.sub('-', '_', i.encode('ascii', 'ignore').lower().strip()) for i in x['ingredients']], axis=1)
testdf['processed_ingredients'] = testdf.apply(lambda x: [re.sub('-', '_', i.encode('ascii', 'ignore').lower().strip()) for i in x['ingredients']], axis=1)
traindf['processed_ingredients_string'] = [','.join(z).strip() for z in traindf['processed_ingredients']]
testdf['processed_ingredients_string'] = [','.join(z).strip() for z in testdf['processed_ingredients']]


# Encode cuisine to integer(start from 0)
le = preprocessing.LabelEncoder()
le.fit(traindf['cuisine'])
traindf['cuisine_number'] = le.transform(traindf['cuisine'])
traindf = traindf.iloc[np.random.permutation(len(traindf))]
cuisine_label = traindf['cuisine_number']
#%%#################### data preprocessing
# Document term matrix
vectorizer = CountVectorizer()
dtm_train = vectorizer.fit_transform(traindf['processed_ingredients_string']).toarray()
dtm_test = vectorizer.transform(testdf['processed_ingredients_string']).toarray()


#tf-idf transforming
tfidf_trans = TfidfTransformer()
tfidf_train = tfidf_trans.fit_transform(dtm_train).toarray()
tfidf_test = tfidf_trans.transform(dtm_test).toarray()
#%%
# Mean validation score: 0.764 (std: 0.002)
clf1 = RandomForestClassifier(bootstrap=False, min_samples_leaf=1, n_estimators=275, min_samples_split=3, criterion='gini', max_features=43, max_depth=None)
# Mean validation score: 0.759 (std: 0.002)
clf2 = RandomForestClassifier(bootstrap=False, min_samples_leaf=1, n_estimators=150, min_samples_split=6, criterion='gini', max_features=40, max_depth=None)
# Mean validation score: 0.765 (std: 0.003)
clf3 = ExtraTreesClassifier(max_features=6, n_estimators=100, criterion='gini', max_depth=None)
# Mean validation score: 0.748 (std: 0.002)
clf4 = ExtraTreesClassifier(max_features=1, n_estimators=70, criterion='entropy', max_depth=None)
# Mean validation score: 0.763 (std: 0.006)
clf5 = SGDClassifier(penalty='l2', loss='log')
# Mean validation score: 0.782 (std: 0.001)
clf6 = SGDClassifier(penalty='l2', loss='modified_huber')
# Mean validation score: 0.781 (std: 0.000)
clf7 = LogisticRegression(penalty='l2', C=1, tol=0.0001, dual=False)
# Mean validation score: 0.786 (std: 0.001)
clf8 = LogisticRegression(penalty='l2', C=8, tol=0.01, dual=False)

num_rounds = 2000
param = {
  'objective':'multi:softprob',
  'eta':0.09,
  'max_depth':5,
  'num_class':20
}
# Mean validation score: 0.8 
clf9 = xgb.train(param, xgb.DMatrix(dtm_train[train_index, :], cuisine_label[train_index]), num_rounds)
# Mean validation score: 0.8 
clf10 = xgb.train(param, xgb.DMatrix(tfidf_train[train_index, :], cuisine_label[train_index]), num_rounds)
# Mean validation score: 0.648 (std: 0.003)
clf11 = KNeighborsClassifier(n_neighbors=16, weights='distance', algorithm='ball_tree', leaf_size=178)
# Mean validation score: 0.747 (std: 0.002)
clf12 = KNeighborsClassifier(n_neighbors=16, weights='distance', algorithm='auto', leaf_size=105)
# Mean validation score: 0.726 (std: 0.001)
clf13 = MultinomialNB(alpha=0.4, fit_prior=True)
# Mean validation score: 0.734 (std: 0.001)
clf14 = MultinomialNB(alpha=0.05, fit_prior=True)
#%% split into 2 disjoint set
n_folds = 2
class_number = 20

skf = list(StratifiedKFold(cuisine_label, n_folds, shuffle=True, random_state=True))
train_index = skf[0][0]
cv_index = skf[0][1]
#%% fit model by first set
#rf on dtm
clf1.fit(dtm_train[train_index, :], cuisine_label[train_index])
#rf on tfidf
clf2.fit(tfidf_train[train_index, :], cuisine_label[train_index])
#ex on dtm
clf3.fit(dtm_train[train_index, :], cuisine_label[train_index])
#ex on tfidf
clf4.fit(tfidf_train[train_index, :], cuisine_label[train_index])
#lsvc on dtm
clf5.fit(dtm_train[train_index, :], cuisine_label[train_index])
#lsvc on tfidf
clf6.fit(tfidf_train[train_index, :], cuisine_label[train_index])
#lr on dtm
clf7.fit(dtm_train[train_index, :], cuisine_label[train_index])
#lr on tfidf
clf8.fit(tfidf_train[train_index, :], cuisine_label[train_index])
#xgb on dtm
##(already fit)
#xgb on tfidf
#(already fit)
##knn on dtm
clf11.fit(dtm_train[train_index, :], cuisine_label[train_index])
#knn on tfidf
clf12.fit(tfidf_train[train_index, :], cuisine_label[train_index])
#multinb on dtm
clf13.fit(dtm_train[train_index, :], cuisine_label[train_index])
#multinb on tfidf
clf14.fit(tfidf_train[train_index, :], cuisine_label[train_index])

#%% level 0 predict as meta-feature
train_pred1 = clf1.predict_proba(dtm_train[cv_index, :])
train_pred2 = clf2.predict_proba(tfidf_train[cv_index, :])
train_pred3 = clf3.predict_proba(dtm_train[cv_index, :])
train_pred4 = clf4.predict_proba(tfidf_train[cv_index, :])
train_pred5 = clf5.predict_proba(dtm_train[cv_index, :])
train_pred6 = clf6.predict_proba(tfidf_train[cv_index, :])
train_pred7 = clf7.predict_proba(dtm_train[cv_index, :])
train_pred8 = clf8.predict_proba(tfidf_train[cv_index, :])
train_pred9 = clf9.predict(xgb.DMatrix(dtm_train[cv_index, :]))
train_pred10 = clf10.predict(xgb.DMatrix(tfidf_train[cv_index, :]))
train_pred11 = clf11.predict_proba(dtm_train[cv_index, :])
train_pred12 = clf12.predict_proba(tfidf_train[cv_index, :])
train_pred13 = clf13.predict_proba(dtm_train[cv_index, :])
train_pred14 = clf14.predict_proba(tfidf_train[cv_index, :])

#%% concatenate level 0 output as level 1 output
blend_train = np.column_stack((train_pred1, train_pred2, train_pred3, train_pred4, train_pred5, train_pred6, train_pred7, train_pred8, train_pred9, train_pred10))

#%% test predict 
test_pred1 = clf1.predict_proba(dtm_test)
test_pred2 = clf2.predict_proba(tfidf_test)
test_pred3 = clf3.predict_proba(dtm_test)
test_pred4 = clf4.predict_proba(tfidf_test)
test_pred5 = clf5.predict_proba(dtm_test)
test_pred6 = clf6.predict_proba(tfidf_test)
test_pred7 = clf7.predict_proba(dtm_test)
test_pred8 = clf8.predict_proba(tfidf_test)
test_pred9 = clf9.predict(xgb.DMatrix(dtm_test))
test_pred10 = clf10.predict(xgb.DMatrix(tfidf_test))
test_pred11 = clf11.predict_proba(dtm_test)
test_pred12 = clf12.predict_proba(tfidf_test)
test_pred13 = clf13.predict_proba(dtm_test)
test_pred14 = clf14.predict_proba(tfidf_test)

#%% concatenate level 0 output as level 1 output
blend_test = np.column_stack((test_pred1, test_pred2, test_pred3, test_pred4, test_pred5, test_pred6, test_pred7, test_pred8, test_pred9, test_pred10))

#%% blend classifier to predict result
param = {
   'objective':'multi:softmax',
   'eta':0.09,
   'max_depth':5,
   'num_class':20
}
num_rounds = 2000

dtrain = xgb.DMatrix(blend_train, cuisine_label[cv_index, :])
dtest = xgb.DMatrix(blend_test)

xgbclf = xgb.train(param, dtrain, num_rounds)
predict_result = xgbclf.predict(dtest)
#%% save model
joblib.dump(clf1, 'RandomForestClassifier_raw_blending.pkl') 
joblib.dump(clf2, 'RandomForestClassifier_tfidf_blending.pkl') 
joblib.dump(clf3, 'ExtraTreesClassifier_raw_blending.pkl') 
joblib.dump(clf4, 'ExtraTreesClassifier_tfidf_blending.pkl') 
joblib.dump(clf5, 'SGDClassifier_raw_blending.pkl') 
joblib.dump(clf6, 'SGDClassifier_tfidf_blending.pkl') 
joblib.dump(clf7, 'LogisticRegression_raw_blending.pkl') 
joblib.dump(clf8, 'LogisticRegression_tfidf_blending.pkl') 
clf9.save_model('xgboost_raw_blending.model')
clf10.save_model('xgboost_tfidf_blending.model')
joblib.dump(clf11, 'KNeighborsClassifier_raw_blending.pkl') 
joblib.dump(clf12, 'KNeighborsClassifier_tfidf_blending.pkl') 
joblib.dump(clf13, 'MultinomialNB_raw_blending.pkl') 
joblib.dump(clf14, 'MultinomialNB_tfidf_blending.pkl') 

#%% save result
np.save("train_index", train_index)
np.save("cv_index", cv_index)

np.save("RandomForestClassifier_raw_predict_train", train_pred1)
np.save("RandomForestClassifier_tfidf_predict_train", train_pred2)
np.save("ExtraTreesClassifier_raw_predict_train", train_pred3)
np.save("ExtraTreesClassifier_tfidf_predict_train", train_pred4)
np.save("SGDClassifier_raw_predict_train", train_pred5)
np.save("SGDClassifier_tfidf_predict_train", train_pred6)
np.save("LogisticRegression_raw_predict_train", train_pred7)
np.save("LogisticRegression_tfidf_predict_train", train_pred8)
np.save("xgboost_raw_predict_train", train_pred9)
np.save("xgboost_tfidf_predict_train", train_pred10)
np.save("KNeighborsClassifier_raw_predict_train", train_pred11)
np.save("KNeighborsClassifier_tfidf_predict_train", train_pred12)
np.save("MultinomialNB_raw_predict_train", train_pred13)
np.save("MultinomialNB_tfidf_predict_train", train_pred14)

np.save("RandomForestClassifier_raw_predict_test", test_pred1)
np.save("RandomForestClassifier_tfidf_predict_test", test_pred2)
np.save("ExtraTreesClassifier_raw_predict_test", test_pred3)
np.save("ExtraTreesClassifier_tfidf_predict_test", test_pred4)
np.save("SGDClassifier_raw_predict_test", test_pred5)
np.save("SGDClassifier_tfidf_predict_test", test_pred6)
np.save("LogisticRegression_raw_predict_test", test_pred7)
np.save("LogisticRegression_tfidf_predict_test", test_pred8)
np.save("xgboost_raw_predict_test", test_pred9)
np.save("xgboost_tfidf_predict_test", test_pred10)
np.save("KNeighborsClassifier_raw_predict_test", test_pred11)
np.save("KNeighborsClassifier_tfidf_predict_test", test_pred12)
np.save("MultinomialNB_raw_predict_test", test_pred13)
np.save("MultinomialNB_tfidf_predict_test", test_pred14)

#%%####################write csv
testdf['cuisine'] = le.inverse_transform(predict_result.astype(np.int32))
predict_dict = dict(zip(testdf['id'], testdf['cuisine']))
with open('predict_result_ensemble.csv', 'w') as csvfile:
    fieldnames = ['id', 'cuisine']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for key, value in predict_dict.iteritems():
        writer.writerow({'id': key, 'cuisine': value})

print("--- %s seconds ---" % (time.time() - start_time))