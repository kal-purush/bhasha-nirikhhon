from tensorflow.keras.layers import Dense, GlobalAveragePooling2D, Dropout
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.applications.inception_resnet_v2 import InceptionResNetV2
from tensorflow.keras.models import Model
from keras.layers import Dropout
import tensorflow as tf
import numpy as np
import os
import glob


test_data_dir = "./images/imagens_cortadas/"
model_path = './models/inception4'

batch_size = 1
img_height = 256
img_width = 600

def classificacao():

        test_datagen = ImageDataGenerator(rescale=1./255)
        for dic in glob.glob(f"{test_data_dir}*"):
                for img_path in glob.glob(os.path.join(dic, "*.jpg")):
                        label = img_path.split("_")

        test_generator = test_datagen.flow_from_directory(
                test_data_dir,
                target_size=(img_height, img_width),
                shuffle=False,
                batch_size=batch_size)

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

        model = tf.keras.models.load_model(model_path)

        predictions = model.predict(test_generator)

        y_pred = np.argmax(predictions, axis=1) + 1

        print(y_pred)