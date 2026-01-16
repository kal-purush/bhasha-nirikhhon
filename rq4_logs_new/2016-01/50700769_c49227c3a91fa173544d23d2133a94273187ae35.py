# Requires sci-kit learn and matplotlib

import matplotlib.pyplot as pyplot

from sklearn import datasets
from sklearn import svm

digits = datasets.load_digits()

#Gamma breaks when greater than 0.01. Maintains high accuracy at 0.001
clf = svm.SVC(gamma=0.001, C=100)

x,y = digits.data[:-1], digits.target[:-1]
clf.fit(x,y)

#Will return a prediction and display the last digit in dataset
print('Prediction:',clf.predict(digits.data[-1]))

pyplot.imshow(digits.images[-1], cmap=pyplot.cm.gray_r, interpolation="nearest")
pyplot.show()