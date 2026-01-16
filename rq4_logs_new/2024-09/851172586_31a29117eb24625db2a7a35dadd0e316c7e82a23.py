from sklearn import svm
from sklearn.tree import DecisionTreeClassifier
import numpy as np
import joblib  # For saving and loading models

# Initialize classifiers
svm_clf = svm.SVC()
dt_clf = DecisionTreeClassifier()

# Function to train classifiers
def train_classifiers(X_train, y_train_svm, y_train_dt):
    svm_clf.fit(X_train, y_train_svm)
    dt_clf.fit(X_train, y_train_dt)
    # Save models for later use
    joblib.dump(svm_clf, 'svm_model.pkl')
    joblib.dump(dt_clf, 'dt_model.pkl')

# Function to load classifiers
def load_classifiers():
    global svm_clf, dt_clf
    svm_clf = joblib.load('svm_model.pkl')
    dt_clf = joblib.load('dt_model.pkl')

def svm_classification(features):
    if svm_clf is None:
        load_classifiers()
    return svm_clf.predict([features])[0]

def decision_tree_classification(features):
    if dt_clf is None:
        load_classifiers()
    return dt_clf.predict([features])[0]