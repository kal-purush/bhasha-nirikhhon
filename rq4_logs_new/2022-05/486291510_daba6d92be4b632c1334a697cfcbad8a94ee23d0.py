from importlib.resources import path
from matplotlib.font_manager import json_load
import json
import os
import tensorflow as tf
import cv2
import pathlib
import numpy as np
import matplotlib.pyplot as plt
import PIL
import PIL.Image
from keras.utils import np_utils
import keras
from yaml import parse
import glob



train_path = r'C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN_Padded_train\\train\\*.png'
test_path = r'C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN_Padded_test\\test\\*.png'

train_images = [cv2.imread(file) for file in glob.glob(train_path)]
test_images = [cv2.imread(file) for file in glob.glob(test_path)]



train_images=train_images
test_images=train_images

plt.imshow(train_images[4],cmap = 'binary')
plt.show()










data_dir = pathlib.Path('C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN_Padded_train')
image_count = len(list(data_dir.glob('*/*.png')))
I = list(data_dir.glob('*/*.png'))
print(image_count) #number of images in file (46470) (test = 0 : 13067) (train = 13068 : end)
s = cv2.imread(str(I[0]))


# create training set
x_train =[]
y_train=[]
for i in range(100):
  if i < 50:
    s = cv2.imread(str(I[i])) 
    x_train.append(s)
  else:
    s = cv2.imread(str(I[i])) 
    y_train.append(s)



x_train = np.asarray(x_train)
x_train = x_train.astype('float32')
y_train = np.asarray(y_train)
y_train = y_train.astype('float32')
#x_test = x_test.astype('float32')
x_train = x_train / 255.0
y_train = y_train / 255.0
#x_test = x_test / 255.0




#test
data_dir = pathlib.Path('C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN_Padded_test')
image_count = len(list(data_dir.glob('*/*.png')))
I = list(data_dir.glob('*/*.png'))
print(image_count) #number of images in file (46470) (test = 0 : 13067) (train = 13068 : end)
s = cv2.imread(str(I[0]))





x_test =[]
y_test=[]
for i in range(20):
  if i < 10:
    s = cv2.imread(str(I[i])) 
    x_test.append(s)
  else:
    s = cv2.imread(str(I[i])) 
    y_test.append(s)







x_test = np.asarray(x_test)
x_test = x_train.astype('float32')
y_test = np.asarray(y_test)
y_test = y_test.astype('float32')
#x_test = x_test.astype('float32')
x_test = x_test / 255.0
y_test = y_test / 255.0
#x_test = x_test / 255.0



########### Code below currently not working ###########

# (X_train, X_test) = train_images
# (y_train, y_test) = test_images

# X_train = X_train/255.0
# X_test = X_test/255.0


#Encoder
encoder_inputs = tf.keras.layers.Input(shape = (516,1083,3), name = 'encoder_inputs')
encoder_flatten = tf.keras.layers.Flatten(name = 'encoder_flatten')(encoder_inputs)
encoder_hidden = tf.keras.layers.Dense(100, activation = 'elu', name = 'encoder_hidden')(encoder_flatten)
encoder_outputs = tf.keras.layers.Dense(30, activation = 'elu', name = 'encoder_output')(encoder_hidden)

encoder = tf.keras.Model(inputs = encoder_inputs, outputs = encoder_outputs, name = 'encoder')

encoder_outputs.shape

encoder.summary()

#Decoder
decoder_inputs = tf.keras.layers.Input(shape = (30,), name = 'decoder_inputs')
decoder_hidden = tf.keras.layers.Dense(100, activation = 'elu', name = 'decoder_hidden')(decoder_inputs)
decoder_outputs = tf.keras.layers.Dense(516*1083*3, activation = 'softmax', name = 'decoder_output')(decoder_hidden)
decoder_outputs_reshape = tf.keras.layers.Reshape([516,1083,3], name = 'decoder_outputs_reshape')(decoder_outputs)

decoder = tf.keras.Model(inputs = decoder_inputs, outputs = decoder_outputs_reshape, name = 'decoder')

decoder.summary()

decoder_outputs.shape

# Autoencoder
autoencoder_inputs = tf.keras.layers.Input(shape = (516,1083,3))
encoded_image = encoder(autoencoder_inputs)
decoded_image = decoder(encoded_image)

autoencoder = tf.keras.Model(inputs = autoencoder_inputs, outputs = decoded_image, name = 'autoencoder')

autoencoder.summary()

autoencoder.compile(loss = 'binary_crossentropy', optimizer = tf.keras.optimizers.Adam(learning_rate = 0.001))

autoencoder.fit(x = x_train, y = x_train, batch_size = 32, epochs = 30)

predautoencoder = autoencoder.predict(y_train)
plt.imshow(y_train[2],cmap = 'binary')
plt.show()
plt.imshow(predautoencoder[2],cmap = 'binary')
plt.show()