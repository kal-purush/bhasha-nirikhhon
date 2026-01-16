# Importing the necessary libraries
import os
from keras.preprocessing import image
import matplotlib.pyplot as plt 
import numpy as np
from keras.utils.np_utils import to_categorical
import random,shutil
from keras.models import Sequential
from keras.layers import Dropout,Conv2D,Flatten,Dense, MaxPooling2D, BatchNormalization
from keras.models import load_model

# Defining the generator function to return batches of data(images)
def generator(dir, gen=image.ImageDataGenerator(rescale=1./255), shuffle=True,batch_size=1,target_size=(24,24),class_mode='categorical' ):

    return gen.flow_from_directory(dir,batch_size=batch_size,shuffle=shuffle,color_mode='grayscale',class_mode=class_mode,target_size=target_size)

# Defining relevant variables    (this is just a sample dataset of a few images for demonstration purpose)
BS= 32          # Batch size
TS=(24,24)      # Target size
train_batch= generator('YawnDS/train',shuffle=True, batch_size=BS,target_size=TS)
valid_batch= generator('YawnDS/valid',shuffle=True, batch_size=BS,target_size=TS)
SPE= len(train_batch.classes)//BS   # Number of steps per epoch
VS = len(valid_batch.classes)//BS   # Validation steps
print(SPE,VS)

# Defining model architecture
model = Sequential([ 
    Conv2D(32, kernel_size=(3, 3), activation='relu', input_shape=(24,24,1)), # 2D Convolutional layer, 32 3x3 filters
    # relu was chosen to provide faster convergence during training and to effectively capture relevant features from data
    Conv2D(32,(3,3),activation='relu'),  # 2D Convolutional layer with 32 3x3 filters, relu activation function
    Conv2D(64, (3, 3), activation='relu'), # 2D Convolutional layer, 64 3x3 filters, relu
    
    Dropout(0.25), # Randomly turn off 25% neurons to prevent overfitting
    Flatten(), # Flatten output to a 1D array (since we only want classification)
    Dense(128, activation='relu'), # Fully connected layer to learn complex relationships and get relevant data
    Dropout(0.5), # One more dropout layer to further prevent overfitting
#output a softmax to squash the matrix into output probabilities
    Dense(4, activation='softmax') # Final layer to produce classification probabilities
    # Here, replace 4 with 2 if you aren't using the yawn eye dataset (in case it throws a Graph execution error)
])

# # Compile, Train and save the model    (Uncomment this cell to train and save the model)
# model.compile(optimizer='adam',loss='categorical_crossentropy',metrics=['accuracy'])
# model.fit(train_batch, validation_data=valid_batch,epochs=15,steps_per_epoch=SPE ,validation_steps=VS)
# model.save('models/ddsCnn.h5', overwrite=True)