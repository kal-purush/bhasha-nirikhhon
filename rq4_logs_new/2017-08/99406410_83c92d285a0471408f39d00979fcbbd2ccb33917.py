import random
import sys
import glob

import cv2
import numpy as np

import keras

#from keras import backend as K
#from keras.models import load_model
#from tensorflow.python.ops import math_ops

#import deep_models as dm


# model = dm.vg16_fcn()
# model.summary()
# model.compile(loss="categorical_crossentropy",
#                  optimizer="adadelta")

"""

def cross_entropy(t, p):
    return -np.sum(t*np.log(p))




true_arr = np.array([[[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]], 
                     [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]],
                     [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]],])


pred_arr = np.array([[[0.9, 0.1], [0.9, 0.1], [0.9, 0.1]], 
                     [[0.9, 0.1], [0.99, 0.01], [0.99, 0.01]],
                     [[0.9, 0.1], [0.99, 0.01], [0.9, 0.01]]])

pred_npr = np.array([[[9.9, 1.1], [8.9, 0.1], [1.9, 6.1]], 
                     [[9.9, 2.1], [9.4, 9.6], [2.5, 5.5]],
                     [[9.9, 1.1], [9.2, 0.8], [3.9, 4.1]]])

print(true_arr.shape)


ce = cross_entropy(true_arr, pred_arr)
print("ce: " + str(ce))

"""


"""
K.get_session()

true_var = K.variable(true_arr)
pred_var = K.variable(pred_arr)
pred_npr = K.variable(pred_npr)

# output = pred_var / math_ops.reduce_sum(pred_var, reduction_indices=len(pred_var.get_shape()) - 1, keep_dims=True)
# print(K.eval(output))

sx = K.softmax(pred_npr)
en = K.categorical_crossentropy(pred_var, true_var, from_logits=False)

sx_eval = K.eval(sx)
print("sx_eval")
print(sx_eval)

en_eval = K.eval(en)
print("en_eval")
print(en_eval)

K.clear_session()
"""

input1 = keras.layers.Input(shape=(16,))
x1 = keras.layers.Dense(8, activation='relu')(input1)
input2 = keras.layers.Input(shape=(32,))
x2 = keras.layers.Dense(8, activation='relu')(input2)
added = keras.layers.Add()([x1, x2])  # equivalent to added = keras.layers.add([x1, x2])

print("added._keras_shape: " + str(added._keras_shape)) 

out = keras.layers.Dense(4)(added)
model = keras.models.Model(inputs=[input1, input2], outputs=out)


