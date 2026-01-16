#!/usr/bin/python


import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D, AveragePooling2D
from keras import backend as K
from PIL import Image
import numpy as np
from sklearn.datasets import load_svmlight_file
from sklearn.metrics import confusion_matrix
import itertools
import matplotlib.pyplot as plt

from sklearn.ensemble import BaggingClassifier
from sklearn.neighbors import KNeighborsClassifier

batch_size = 128
num_classes = 10
epochs = 40

import matplotlib.pyplot as plt

X_train, y_train = load_svmlight_file('./dataset/digTrain20k.txt')
X_test, y_test = load_svmlight_file('./dataset/digTest58k.txt')

## save for the confusion matrix
label = y_test
print(y_test)

bagging = BaggingClassifier(KNeighborsClassifier(), max_samples=0.5, max_features=0.5)

print('Fitting...')
history = bagging.fit(X_train, y_train)
print('Score...')
y_pred = bagging.score(X_test, label)		
cm = confusion_matrix(label, y_pred)
print (cm)

# import matplotlib.pyplot as plt
# from plot import plot_confusion_matrix

# # # Compute confusion matrix
# # cnf_matrix = confusion_matrix(y_test, y_pred)
# np.set_printoptions(precision=2)

# # Plot non-normalized confusion matrix
# # plt.figure()
# plot_confusion_matrix(cm, classes=range(0,10), title='Confusion matrix, without normalization')

# # # Plot normalized confusion matrix
# # plt.figure()
# # plot_confusion_matrix(cnf_matrix, classes=class_names, normalize=True,
# #                       title='Normalized confusion matrix')

# plt.show()