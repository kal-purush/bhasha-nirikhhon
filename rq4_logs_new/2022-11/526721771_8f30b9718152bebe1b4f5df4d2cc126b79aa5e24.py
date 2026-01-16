from tensorflow.keras.layers import Conv2D, Flatten, Dense, MaxPool2D, BatchNormalization, GlobalAveragePooling2D, Activation, Dropout, MaxPooling2D
from tensorflow.keras.applications. resnet50 import preprocess_input, decode_predictions
from tensorflow.keras.preprocessing.image import ImageDataGenerator, load_img
from tensorflow.keras.applications.resnet50 import ResNet50
from tensorflow.keras.applications.inception_resnet_v2 import InceptionResNetV2
from tensorflow.keras.preprocessing import image
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.losses import CategoricalCrossentropy
from tensorflow.keras.regularizers import l2
from tensorflow.keras.callbacks import ReduceLROnPlateau, EarlyStopping
from tensorflow import optimizers
from sklearn.utils import shuffle

import seaborn as sns
from tensorflow.keras.layers import Flatten
from sklearn.metrics import confusion_matrix
from keras.api._v2.keras import callbacks
from keras.layers import Dropout
import tensorflow as tf
import matplotlib.pyplot as plt
import numpy as np
import warnings
from sklearn.metrics import confusion_matrix, classification_report
import pathlib
import os
import glob
print(pathlib.Path().resolve()) 

test_data_dir = "/home/luan/Desktop/classificação/imagens_cortadas/"

batch_size = 32
img_height = 256
img_width = 600

test_datagen = ImageDataGenerator(rescale=1./255)
for dic in glob.glob(f"{test_data_dir}*"):
    for img_path in glob.glob(os.path.join(dic, "*.jpg")):
        label = img_path.split("_")
        print(label)


test_generator = test_datagen.flow_from_directory(
        test_data_dir,
        target_size=(256, 600),
        shuffle=False,
        batch_size=1)





base_model = InceptionResNetV2(include_top=False, weights="imagenet")
x = base_model.output
x = GlobalAveragePooling2D()(x)
x = Dense(1024, activation='sigmoid')(x)
x = Dense(128, activation='relu')(x)
x = Dropout(0.2)(x)
predictions = Dense(7, activation='softmax')(x)
model = Model(inputs=base_model.input, outputs=predictions)


labels = {0: "classe_1",
          1: "classe_2",
          2: "classe_3",
          3: "classe_4",
          4: "classe_5",
          5: "classe_6",
          6: "classe_7"}

model.load_weights('inception4')

predictions = model.predict(test_generator)

y_pred = np.argmax(predictions, axis=1) + 1

print(y_pred)