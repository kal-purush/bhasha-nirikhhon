"""
=================================
Gaussian Mixture Model Ellipsoids
=================================

Plot the confidence ellipsoids of a mixture of two gaussians with EM
and variational dirichlet process.

Both models have access to five components with which to fit the
data. Note that the EM model will necessarily use all five components
while the DP model will effectively only use as many as are needed for
a good fit. This is a property of the Dirichlet Process prior. Here we
can see that the EM model splits some components arbitrarily, because it
is trying to fit too many components, while the Dirichlet Process model
adapts it number of state automatically.

This example doesn't show it, as we're in a low-dimensional space, but
another advantage of the dirichlet process model is that it can fit
full covariance matrices effectively even when there are less examples
per cluster than there are dimensions in the data, due to
regularization properties of the inference algorithm.
"""

import itertools

import numpy as np
from scipy import linalg
import pylab as pl
import matplotlib as mpl
from sklearn import mixture
from sklearn.externals import joblib
from sklearn.svm import SVC

def ModelTraining(tag,feature_matrix,gameID):
 
  #VBGMM
  g = mixture.GMM(n_components=2, covariance_type = 'tied')
  g.fit(feature_matrix)
  joblib.dump(g, 'GMM_model_' + str(gameID) + '/'+ tag +'_model.pkl') #NAME OF FOLDER TO SAVE THE NEW RETRAINED MODELS
  
def ModelTrainingSVM(tag,feature_matrix,labels,gameID):
  svm = SVC()
  svm.fit(feature_matrix, labels)
  joblib.dump(svm, 'SVM_model_' + str(gameID) + '/' + tag + '_model.pkl') #NAME OF FOLDER TO SAVE RETRAINED SVM MODELS
  
