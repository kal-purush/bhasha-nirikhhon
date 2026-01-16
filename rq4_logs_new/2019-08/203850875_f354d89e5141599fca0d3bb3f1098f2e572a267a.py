import numpy as np
import pandas as pd
from keras.models import Sequential
from keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
from numpy import ndarray

from subprocess import check_output


data = pd.read_csv('/mnt/vboxsync/sandp500/all_stocks_5yr.csv')
cl = data[data['Name']=='MMM'].close
scl = MinMaxScaler()
cl = cl.values.reshape(cl.shape[0],1)
cl = scl.fit_transform(cl)
print(cl.shape)


def process_data(data, lb:int):
    x, y = [], []
    for i in range(len(data)-lb-1):
        # one data has 7 data points. 
        x.append(data[i:i+lb, 0])
        y.append(data[i+lb, 0])
    return np.array(x), np.array(y)

x, y = process_data(cl, 7)

# use first 80% data as train, last 20% data as test.
x_train, x_test = x[:int(x.shape[0]*0.80)], x[int(x.shape[0]*0.80):]
y_train, y_test = y[:int(y.shape[0]*0.80)], y[int(y.shape[0]*0.80):]


# Create a model.
model = Sequential()
model.add(LSTM(256, input_shape=(7,1)))
model.add(Dense(1))
model.compile(optimizer='adam', loss='mse')