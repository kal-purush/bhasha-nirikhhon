import os
import pandas as pd
import numpy as np
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, ENGLISH_STOP_WORDS
from sklearn.decomposition import TruncatedSVD
from sklearn.svm import LinearSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn import linear_model
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, accuracy_score, recall_score, precision_score, f1_score
from sklearn.model_selection import KFold
import statistics



def split(df):
	X, y = [], []
	X = df.Tokenized.tolist()
	y = df.Label.tolist()
	X, y = np.array(X), np.array(y)
	print ("total examples %s" % len(y))
	return X, y, df.Label.unique()

def load(file):
	train_data = pd.read_csv('/kaggle/input/question1/train.csv')
	train_data = train_data.head(n=30000)
	train_data['Tokenized'] = train_data['Content'].apply(word_tokenize)
	return split(train_data)



#Loading train data
X, y, labels = load('/kaggle/input/question1/train.csv')


#Defining all models

svm_bow = Pipeline([
	("BoW", CountVectorizer(analyzer=lambda x: x)), 
	("SVM", LinearSVC(max_iter=10000))
	])
svm_svd = Pipeline([
	('vect', CountVectorizer(analyzer=lambda x: x)),
	('feat', TruncatedSVD(n_components = 600)),
	("SVM", LinearSVC(max_iter=10000))
	])
rf_bow = Pipeline([
	("BoW", CountVectorizer(analyzer=lambda x: x)), 
	("Random Forest", RandomForestClassifier(n_estimators=100))
	])
rf_svd = Pipeline([
	('vect', CountVectorizer(analyzer=lambda x: x)),
	('feat', TruncatedSVD(n_components = 600)),
	("Random Forest", RandomForestClassifier(n_estimators=100))
	])
my_method = Pipeline([
	("TFIDF", TfidfVectorizer(stop_words = ENGLISH_STOP_WORDS, max_df = 0.7, analyzer=lambda x: x)),
	("SVM", LinearSVC(max_iter=10000))
	])

models=[
	("SVM (BoW)", svm_bow),
	("SVM (SVD)", svm_svd),
	("Random Forest (BoW)", rf_bow),
	("Random Forest (SVD)", rf_svd),
	("My Method", my_method)
]


d = {'Statistic Measure': ['Accuracy', 'Precision', 'Recall', 'F-Measure']}
evaluation = pd.DataFrame(data=d).set_index('Statistic Measure')

kf = KFold(n_splits=5)
for name, model in models:
	s = pd.Series()
	for train, test in kf.split(X, y):
		X_train, X_test = X[train], X[test]
		y_train, y_test = y[train], y[test]
		train = model.fit(X_train, y_train)
		ypred = model.predict(X_test)

	d = [accuracy_score(y_test, ypred), precision_score(y_test, ypred, average = 'macro'), recall_score(y_test, ypred, average = 'macro'), f1_score(y_test, ypred, average = 'macro')]
	evaluation[name] = d


evaluation.to_csv('/kaggle/working/Evaluation_5Fold_q1.csv', sep = ',')

#Loading test data
test_data = pd.read_csv('/kaggle/input/question1/test_without_labels.csv')
test_data = test_data.head(n=10000)
test_data['Tokenized'] = test_data['Content'].apply(word_tokenize)
cont, id_n = [], []
cont = test_data.Tokenized.tolist()
id_n = test_data.Id.tolist()


#Predicting Labels
train = my_method.fit(X, y)
predictions = my_method.predict(cont)


prediction_table = pd.DataFrame(data = id_n, columns = ['Id'])
prediction_table['Predicted'] = predictions
prediction_table.to_csv('/kaggle/working/testSet_categories.csv', sep =',', index=False)