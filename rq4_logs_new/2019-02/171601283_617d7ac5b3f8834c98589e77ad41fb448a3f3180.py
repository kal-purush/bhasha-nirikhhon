Code

import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from sklearn.linear_model import LogisticRegression

from sklearn import tree
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier



# to read the dataset
train_data = pd.read_csv(r"/Users/sumi/python/train.csv", encoding = "ISO-8859-1")
#features
features = train_data.drop('project_is_approved', 1)
#labels
target = train_data[['project_is_approved']]

#split the train and test data
X_train, X_test, y_train, y_test = train_test_split(features, target, random_state =67)
 

X_train['full_essay'] = X_train['project_essay_1'] + ' ' + X_train['project_essay_2'] + ' ' + X_train['project_resource_summary']
X_test['full_essay'] = X_test['project_essay_1'] + ' ' + X_test['project_essay_2'] + ' ' + X_test['project_resource_summary']

#
#vectorizer = CountVectorizer(min_df = 50 , stop_words = "english").fit(X_train['full_essay'])
##vectorizer.fit(X_train['full_essay'])
#X_train_vec = vectorizer.transform(X_train['full_essay'])
#X_test_vec = vectorizer.transform(X_test['full_essay'])



#########################################################################

print('Count vectorizer')
print('Decision tree classifier')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5)), ("tree", tree.DecisionTreeClassifier())])
param_grid = {'tree__max_leaf_nodes' : [ 100],
              'tree__max_depth' : [50],
              'tree__max_features' : [ 100]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Count vectorizer')
print('Logistic regression')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5, stop_words = "english")), ("cls", LogisticRegression())])
param_grid = {'cls__C' : [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Count vectorizer')
print('Random forest classifier')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5)), ("forest", RandomForestClassifier())])
param_grid = {'forest__n_estimators' : [ 100],
              'forest__max_depth' : [50]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Count vectorizer')
print('Multilayer perceptron')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5)), ("mlp", MLPClassifier())])
param_grid = {'mlp__hidden_layer_sizes': [(100,100)],
        'mlp__alpha': [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

###################################################################################

print('Count vectorizer with stopwords')
print('Decision tree classifier')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5, stop_words = "english")), ("tree", tree.DecisionTreeClassifier())])
param_grid = {'tree__max_leaf_nodes' : [ 100],
              'tree__max_depth' : [50],
              'tree__max_features' : [ 100]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Count vectorizer with stopwords')
print('Logistic regression')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5, stop_words = "english")), ("cls", LogisticRegression())])
param_grid = {'cls__C' : [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Count vectorizer with stopwords')
print('Random forest classifier')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5, stop_words = "english")), ("forest", RandomForestClassifier())])
param_grid = {'forest__n_estimators' : [ 100],
              'forest__max_depth' : [50]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Count vectorizer with stopwords')
print('Multilayer perceptron')
pipe = Pipeline([("scaler", CountVectorizer(min_df = 5, stop_words = "english")), ("mlp", MLPClassifier())])
param_grid = {'mlp__hidden_layer_sizes': [(100,100)],
        'mlp__alpha': [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))


##############################################################################################

print('Tf-idf')
print('Decision tree classifier')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5)), ("tree", tree.DecisionTreeClassifier())])
param_grid = {'tree__max_leaf_nodes' : [ 100],
              'tree__max_depth' : [50],
              'tree__max_features' : [ 100]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Tf-idf')
print('Logistic regression')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5)), ("cls", LogisticRegression())])
param_grid = {'cls__C' : [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Tf-idf')
print('Random forest classifier')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5)), ("forest", RandomForestClassifier())])
param_grid = {'forest__n_estimators' : [ 100],
              'forest__max_depth' : [50]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Tf-idf with stopwords')
print('Multilayer perceptron')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5)), ("mlp", MLPClassifier())])
param_grid = {'mlp__hidden_layer_sizes': [(100,100)],
        'mlp__alpha': [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))





##############################################################################################

print('Tf-idf with stopwords')
print('Decision tree classifier')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5, stop_words = "english")), ("tree", tree.DecisionTreeClassifier())])
param_grid = {'tree__max_leaf_nodes' : [ 100],
              'tree__max_depth' : [50],
              'tree__max_features' : [ 100]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))


print('Tf-idf with stopwords')
print('Logistic regression')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5, stop_words = "english")), ("cls", LogisticRegression())])
param_grid = {'cls__C' : [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Tf-idf with stopwords')
print('Random forest classifier')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5, stop_words = "english")), ("forest", RandomForestClassifier())])
param_grid = {'forest__n_estimators' : [ 100],
              'forest__max_depth' : [50]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Tf-idf with stopwords')
print('Multilayer perceptron')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5, stop_words = "english")), ("mlp", MLPClassifier())])
param_grid = {'mlp__hidden_layer_sizes': [(10,10)],
        'mlp__alpha': [0.1]}
grid = GridSearchCV(pipe, param_grid = param_grid, cv = 3)
grid.fit(X_train['full_essay'],y_train )
pred=  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))

print('Bag-of-words with n-gram')
pipe = Pipeline([("scaler", TfidfVectorizer(min_df = 5)), ("cls", LogisticRegression())])
param_grid = { "cls__C" : [0.1],
"scaler__ngram_range" : [(1 , 2), (1 , 3)]}
grid = GridSearchCV ( pipe , param_grid , cv = 3 )
grid.fit(X_train['full_essay'] , y_train)
pred =  grid.predict(X_test['full_essay'])
print("Test set accuracy: {}".format(grid.score(X_test['full_essay'],y_test)))
print("f1 measure: {}".format(classification_report(y_test,pred)))
print ( "Best cross-validation score: {:.2f}" . format ( grid.best_score_ ))
print ( "Best parameters: \n {}" . format ( grid.best_params_ ))