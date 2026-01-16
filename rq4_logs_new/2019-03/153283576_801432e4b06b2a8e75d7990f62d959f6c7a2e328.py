#TensorFlow CNN testing
#Varying models of CNN with training data
#Assessing perfomance with TensorBoard

import tensorflow as tf
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Activation, Flatten, Conv2D, MaxPooling2D
from tensorflow.keras import backend as K
from tensorflow.keras.callbacks import TensorBoard
import time
import pickle

#loading in training data
pickle_in = open("generated_data_1\\X_scaled.pickle","rb")
X = pickle.load(pickle_in)
pickle_in.close()

pickle_in = open("generated_data_1\\y2.pickle","rb")
y = pickle.load(pickle_in)
pickle_in.close()

#TensorFlow part of code

#model variations
dense_layers = [1,2]
dense_layer_sizes = [32, 64, 128]
conv_layer_sizes = [32, 64]
conv_layers = [3]

#looping over all model variations
for dense_layer_size in dense_layer_sizes:
    for dense_layer in dense_layers:
        for conv_layer_size in conv_layer_sizes:
            for conv_layer in conv_layers:
                Name = f"{conv_layer}_conv_{conv_layer_size}_nodes_{dense_layer}_dense_{dense_layer_size}_nodes_{int(time.time())}"
                print(Name)
                #TensorBoard callback
                
                tensorboard = TensorBoard(log_dir=f"logs2/{Name}")

                model = Sequential()
                
                model.add(Conv2D(conv_layer_size, (3,3), input_shape = X.shape[1:]))
                model.add(Activation("relu"))
                model.add(MaxPooling2D(pool_size=(2,2)))
                
                for l in range(conv_layer-1):
                    model.add(Conv2D(conv_layer_size, (3,3)))
                    model.add(Activation("relu"))
                    model.add(MaxPooling2D(pool_size=(2,2)))
                
                model.add(Flatten())
                for l in range(dense_layer):
                    model.add(Dense(dense_layer_size))
                    model.add(Activation("relu"))
                
                model.add(Dense(2))
                model.add(Activation("softmax"))
                
                model.compile(loss="sparse_categorical_crossentropy",
                              optimizer="adam",
                              metrics=["accuracy"])
                
                model.fit(X, y, epochs=12, callbacks=[tensorboard], validation_split=0.2) ##validate here for tensorboard callback or use callback param in evaluate
               
    
    
    




