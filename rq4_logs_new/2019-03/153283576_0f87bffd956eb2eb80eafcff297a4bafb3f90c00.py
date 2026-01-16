#Formating Training Data
#reshape the constructed matrix files for use in CNN 
#saving with pickle

import numpy as np
import os
import random
import pickle
from sklearn import preprocessing

DATADIR = os.getcwd() + "\matrix_files\\Playground3\\"
training_data=[]


CATEGORIES=["Noise","Burst"]

def create_training_data():
    for category in CATEGORIES:
        path = os.path.join(DATADIR,category)
        class_num = CATEGORIES.index(category)
        for matrix in os.listdir(path):
            #if len(training_data)==40:
                #break
            try:
                matrix_arr = np.load(os.path.join(path,matrix))
                training_data.append([matrix_arr, class_num])
            except:
                pass
       
create_training_data()      

random.shuffle(training_data)

X = []
y = []

for features, label in training_data:
    X.append(features)
    y.append(label)

X = np.array(X).reshape(-1, 70, 100, 1)
X = X/np.amax(X)
"""
scaler = preprocessing.MinMaxScaler()
X_scaled = scaler.fit_transform(X)
"""

pickle_out = open("X_scaled.pickle","wb")
pickle.dump(X,pickle_out)
pickle_out.close()

pickle_out = open("y2.pickle","wb")
pickle.dump(y, pickle_out)
pickle_out.close()

