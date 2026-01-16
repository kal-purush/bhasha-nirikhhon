# Import necessary modules
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt

from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Create feature and target arrays
data = pd.read_csv('supervised_small.csv')
phone_x_accelerometer = "raw_acc:3d:std_x"
phone_y_accelerometer = "raw_acc:3d:std_y"
running = "label:FIX_running"
walking = "label:FIX_walking"

X = data[[phone_x_accelerometer,phone_y_accelerometer]]
Y = data[[running, walking]]
# Split into training and test set
X_train, X_test, y_train, y_test = train_test_split(
			X, Y, test_size = 0.2, random_state=42)

knn = KNeighborsClassifier(n_neighbors=7)

knn.fit(X_train, y_train)

# Predict on dataset which model has not seen before
print(accuracy_score(knn.predict(X_test), y_test))
#print(knn.predict(X_test))

