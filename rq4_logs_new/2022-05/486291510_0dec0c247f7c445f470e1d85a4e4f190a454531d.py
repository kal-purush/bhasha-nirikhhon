from json import encoder
import tensorflow as tf
import matplotlib.pyplot as plt
import numpy as np
import cv2
import glob

train_path = r'C:\Users\jwade\OneDrive - University of Tennessee\Masters Program\BZAN 554 - Deep Learning for Business\Group Assignment - 3\train\*.png'

test_path = r'C:\Users\jwade\OneDrive - University of Tennessee\Masters Program\BZAN 554 - Deep Learning for Business\Group Assignment - 3\test\*.png'

train_images = [cv2.imread(file) for file in glob.glob(train_path)]
test_images = [cv2.imread(file) for file in glob.glob(test_path)]

plt.imshow(train_images[4],cmap = 'binary')
plt.show()

########### Code below currently not working ###########

# (X_train, X_test) = train_images
# (y_train, y_test) = test_images

# X_train = X_train/255.0
# X_test = X_test/255.0


#Encoder
encoder_inputs = tf.keras.layers.Input(shape = (28,28), name = 'encoder_inputs')
encoder_flatten = tf.keras.layers.Flatten(name = 'encoder_flatten')(encoder_inputs)
encoder_hidden = tf.keras.layers.Dense(100, activation = 'elu', name = 'encoder_hidden')(encoder_flatten)
encoder_outputs = tf.keras.layers.Dense(30, activation = 'elu', name = 'encoder_output')(encoder_hidden)

encoder = tf.keras.Model(inputs = encoder_inputs, outputs = encoder_outputs, name = 'encoder')

encoder_outputs.shape

encoder.summary()

#Decoder
decoder_inputs = tf.keras.layers.Input(shape = (30,), name = 'decoder_inputs')
decoder_hidden = tf.keras.layers.Dense(100, activation = 'elu', name = 'decoder_hidden')(decoder_inputs)
decoder_outputs = tf.keras.layers.Dense(28*28, activation = 'sigmoid', name = 'decoder_output')(decoder_hidden)
decoder_outputs_reshape = tf.keras.layers.Reshape([28,28], name = 'decoder_outputs_reshape')(decoder_outputs)

decoder = tf.keras.Model(inputs = decoder_inputs, outputs = decoder_outputs_reshape, name = 'decoder')

decoder.summary()

decoder_outputs.shape

# Autoencoder
autoencoder_inputs = tf.keras.layers.Input(shape = (28,28))
encoded_image = encoder(autoencoder_inputs)
decoded_image = decoder(encoded_image)

autoencoder = tf.keras.Model(inputs = autoencoder_inputs, outputs = decoded_image, name = 'autoencoder')

autoencoder.summary()

autoencoder.compile(loss = 'binary_crossentropy', optimizer = tf.keras.optimizers.Adam(learning_rate = 0.001))

autoencoder.fit(x = X_train, y = X_train, batch_size = 32, epochs = 30)