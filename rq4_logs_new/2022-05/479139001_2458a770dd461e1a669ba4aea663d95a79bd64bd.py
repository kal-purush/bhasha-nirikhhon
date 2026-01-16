import os
import zipfile
import requests
import tarfile
import scipy.io as scio
import shutil
import pandas as pd
import numpy as np
from numpy import expand_dims
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from skimage.transform import resize
from IPython.display import SVG
from tensorflow import keras
from keras import applications
from keras import optimizers
from keras.models import Model, Sequential
from keras.preprocessing.image import ImageDataGenerator, load_img, img_to_array
from keras.layers import Dense, Flatten, Dropout, BatchNormalization, GlobalAveragePooling2D
from keras.applications.inception_v3 import InceptionV3, preprocess_input
from keras.utils.np_utils import to_categorical
from keras import layers, models
from keras.callbacks import ModelCheckpoint, EarlyStopping, CSVLogger, ReduceLROnPlateau


images_path = "../images.tar"

# image.tar file url
dataset_url = "http://vision.stanford.edu/aditya86/ImageNetDogs/images.tar"

# download the image.tar and write in the local path
res = requests.get(url=dataset_url)
with open(images_path,mode='wb') as f:
    f.write(res.content)

# untar the file
def un_tar(file_name):
      tar = tarfile.open(file_name)
      names = tar.getnames()
      if os.path.isdir(file_name + "_files"):
          pass
      else:
          os.mkdir(file_name + "_files")
      for name in names:
          tar.extract(name, file_name + "_files/")
      tar.close()
un_tar(images_path)

# download the classification of train and test list
classification_url = "http://vision.stanford.edu/aditya86/ImageNetDogs/lists.tar"

classification_path = "../lists.tar"

# write the list in local path
classification_res = requests.get(url=classification_url)

with open(classification_path,mode='wb') as f:
    f.write(classification_res.content)

un_tar(classification_path)

# load the test and train classification list
test_path = "../lists.tar_files/test_list.mat"
train_path = "../lists.tar_files/train_list.mat"

# images path (untar by images.tar)
path_img = "../images.tar_files/"
if not os.path.exists(path_img+"test"):
    os.makedirs(path_img+"test")
if not os.path.exists(path_img+"train"):
    os.makedirs(path_img+"train")

test_data = scio.loadmat(test_path)

# create test folder
for t in test_data["labels"]:
    if not os.path.exists(path_img+"test/"+str(t[0])):
        os.makedirs(path_img+"test/"+str(t[0]))
train_data = scio.loadmat(train_path)

# create train folder
for t in train_data["labels"]:
    if not os.path.exists(path_img+"train/"+str(t[0])):
        os.makedirs(path_img+"train/"+str(t[0]))

# classify the test images to test folder
for t in range(len(test_data["file_list"])):
    source_path = (path_img+"Images/"+test_data["file_list"][t][0][0])
    target_path = (path_img+"test/"+str(test_data["labels"][t][0])+"/"+test_data["file_list"][t][0][0].split("/", 1)[1])
    shutil.copyfile(source_path,target_path)

# classify the train images to test folder
for t in range(len(train_data["file_list"])):
    source_path = (path_img+"Images/"+train_data["file_list"][t][0][0])
    target_path = (path_img+"train/"+str(train_data["labels"][t][0])+"/"+train_data["file_list"][t][0][0].split("/", 1)[1])
    shutil.copyfile(source_path,target_path)

test_path = "../images.tar_files/test"
train_path = "../images.tar_files/train"

img_width, img_height = 224, 224
channels = 3
batch_size = 64
num_images = 100
image_arr_size = img_width * img_height * channels

# Data preprocessing: use the ImageDataGenerator
# Data augmentation has been proposed: rescale, shear, zoom, flip, rotation, shift
# These augmentation can boost the classification quality
# divide training generator and validation generator
train_datagen = ImageDataGenerator(
    rescale= 1./255,
    shear_range= 0.2,
    zoom_range= 0.2,
    horizontal_flip= True,
    rotation_range= 20,
    width_shift_range= 0.2,
    height_shift_range= 0.2,
    validation_split=0.2,

)

valid_datagen = ImageDataGenerator(
    rescale= 1./255,
    validation_split=0.2,
)


train_generator = train_datagen.flow_from_directory(
    train_path,
    target_size= (img_width, img_height),
    color_mode= 'rgb',
    batch_size= batch_size,
    class_mode= 'categorical',
    subset='training',
    shuffle= True,
    seed= 1337
)

valid_generator = valid_datagen.flow_from_directory(
    train_path,
    target_size= (img_width, img_height),
    color_mode= 'rgb',
    batch_size= batch_size,
    class_mode= 'categorical',
    subset='validation',
    shuffle= True,
    seed= 1337
)

# define the number of labels
num_classes = len(train_generator.class_indices)
train_labels = train_generator.classes
train_labels = to_categorical(train_labels, num_classes=num_classes)
valid_labels = valid_generator.classes
valid_labels = to_categorical(valid_labels, num_classes=num_classes)
nb_train_samples = len(train_generator.filenames)
nb_valid_samples = len(valid_generator.filenames)

# test generator do not need to data augmentation
test_datagen = ImageDataGenerator(rescale=1/255.0)
test_generator = test_datagen.flow_from_directory(
                                    test_path,
                                    target_size=(img_width, img_height),
                                    color_mode= 'rgb',
                                    batch_size=batch_size,
                                    class_mode= 'categorical',
                                    shuffle=False,)
test_generator.reset()


# create the InceptionV3 model
InceptionV3 = InceptionV3(include_top= False, input_shape= (img_width, img_height, channels), weights= 'imagenet')
# InceptionV3.summary()

model = Sequential()

for layer in InceptionV3.layers:
    layer.trainable = False

# add some parameter of this model
model.add(InceptionV3)
model.add(layers.Dropout(0.3))
model.add(layers.Flatten())
model.add(layers.BatchNormalization())
model.add(layers.Dense(1000,kernel_initializer='he_uniform'))
model.add(layers.BatchNormalization())
model.add(layers.Activation('relu'))
model.add(layers.Dense(120,activation='softmax'))
model.summary()

# print some information of this model by training
model.compile(optimizer='adam',
              loss='categorical_crossentropy',
              metrics=['accuracy'])
history = model.fit(train_generator, steps_per_epoch=50, epochs=10, verbose= 1, validation_data= valid_generator)

# predict
pred = model.predict(test_generator, verbose=2)
pred = np.argmax(pred, axis=1)
test_labels = test_generator.classes
test_labels = to_categorical(test_labels, num_classes=num_classes)

# Compare with the array of labels to calculate the accuracy of the test set
# Because print the percentage, so should multiply by 100
test_accuracy = 100*np.sum(pred == np.argmax(test_labels, axis=1))/len(pred)
print('Test accuracy: %.4f%%' % test_accuracy)

score = model.evaluate(test_generator, verbose=2)