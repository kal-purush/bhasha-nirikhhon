__author__ = 'jliu2188'
import numpy as np
from sklearn.svm import LinearSVC
from sklearn.datasets import make_classification

# Build a classification task using 3 informative features
X, y = make_classification(n_samples=1000, n_features=25, n_informative=3,
                           n_redundant=2, n_repeated=0, n_classes=8,
                           n_clusters_per_class=1, random_state=0)
print X.shape
#X_new = LinearSVC(C=0.01, penalty="l1", dual=False).fit_transform(X, y)
clf = LinearSVC(C=0.01, penalty="l1", dual=False).fit(X,y)
X_new = clf.fit_transform(X,y)
array = clf.coef_
print np.nonzero(np.apply_along_axis(np.sum,0,array))[0] # a tuple that includes indices of selected variables
print X_new.shape