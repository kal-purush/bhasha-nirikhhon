#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: Jia ShiLin

import numpy as np
np.random.seed(10)

from keras.models import Sequential
from keras.layers import Dense
from keras.models import load_model

#data
# create some data
X = np.linspace(-1, 1, 200)
np.random.shuffle(X)    # randomize the data
Y = 0.5 * X + 2 + np.random.normal(0, 0.05, (200, ))
X_train, Y_train = X[:160], Y[:160]     # first 160 data points
X_test, Y_test = X[160:], Y[160:]       # last 40 data points

model = Sequential()
# model.add(Dense(output_dim =1,input_dim=1))
# model.compile(loss='mse',optimizer='sgd')
# for step in range(201):
#     cost = model.train_on_batch(X_train,Y_train)

# #model_save
# print('before predict:',model.predict(X_test[0:5]))
# model.save('my_model.h5')

#model_load
model = load_model('my_model.h5')
print('after predict:',model.predict(X_test[0:5]))

#另外的方法
"""
# save and load weights
model.save_weights('my_model_weights.h5')
model.load_weights('my_model_weights.h5')

# save and load fresh network without trained weights
from keras.models import model_from_json
json_string = model.to_json()
model = model_from_json(json_string)
"""