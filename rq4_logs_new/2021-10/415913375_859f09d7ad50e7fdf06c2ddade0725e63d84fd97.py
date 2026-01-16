from tensorflow.keras.datasets import mnist
from tensorflow.keras.layers import Dense, Input, Flatten,Reshape, Conv2D
from tensorflow.keras.models import Model
import tensorflow as tf
import numpy as np

def build_model():
    input_layer=Input((28,28),name='encoder_input')
    reshape_layer=Reshape((28,28,1),name='encoder_reshape')(input_layer)
    conv_1=Conv2D(32, 3, activation='relu', padding='same', strides=(2,2),name='conv_1')(reshape_layer)
    conv_2=Conv2D(64, 3, activation='relu', padding='same', strides=(2,2),name='conv_2')(conv_1)
    conv_2=Conv2D(128, 3, activation='relu', padding='same', strides=(2,2),name='conv_3')(conv_2)
    flatten_layer=Flatten(name='flatten')(conv_3)
    output_layer=Dense(10,activation='sigmoid',name='output')(flatten_layer)

    model=Model(inputs=input_layer,outputs=output_layer)
    model.compile(optimizer='Adam',loss='binarycrossentropy')

    return model

def train_model(model,x_train,y_train,val_data,epochs=10,batch_size=128,save_file=None,load_file=None):
    if load_file is not None:
        model.load_weights(load_file+'.h5')
    model.fit(x_train, y_train,val_data,epochs=epochs,batch_size=batch_size)

    if save_file is not None:
        model.save_weights(save_file+'.h5')
    