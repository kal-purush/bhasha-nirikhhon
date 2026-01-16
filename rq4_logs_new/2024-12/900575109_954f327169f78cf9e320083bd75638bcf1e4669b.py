
# Automating Airplane Detection in Satellite Imagery


# Import the necessary libraries
import os
import numpy as np
import matplotlib.pyplot as plt
import tensorflow

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv2D, MaxPooling2D, Flatten, Dense, Dropout
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from sklearn.model_selection import train_test_split
from PIL import Image

# Load and preprocess the data
def load_images_from_folder(folder):
    images = []
    labels = []  # 1 for 'plane' and 0 for 'no-plane'
    for filename in os.listdir(folder):
        img = Image.open(os.path.join(folder, filename))
        if img is not None:
            img = img.resize((20, 20))  # Resize to make sure all images are the same size
            images.append(np.array(img))
            # Extract label from filename
            label = 1 if '1__' in filename else 0
            labels.append(label)
    return np.array(images), np.array(labels)

# Images are in the 'planesnet' directory assigned to filepath
#filepath = ('C:\Users\Admin\Desktop\Jackson\FIU\Graduate School\CAP 5610 Introduction to Machine Learning\Final Project\Satellite_Airplane_Image\planesnet\planesnet')
filepath = r'C:\Users\Admin\Desktop\Jackson\FIU\Graduate School\CAP 5610 Introduction to Machine Learning\Final Project\Satellite_Airplane_Image\planesnet\planesnet'
images, labels = load_images_from_folder(filepath)
images = images / 255.0  # Normalizing pixel values

# Split dataset into training and testing
x_train, x_test, y_train, y_test = train_test_split(images, labels, test_size=0.2, random_state=42)

# Data augmentation
train_datagen = ImageDataGenerator(
    rotation_range=20,
    width_shift_range=0.2,
    height_shift_range=0.2,
    shear_range=0.2,
    zoom_range=0.2,
    horizontal_flip=True,
    fill_mode='nearest'
)

train_generator = train_datagen.flow(x_train, y_train, batch_size=32)

# Model definition
def create_model():
    model = Sequential([
        Conv2D(32, (3, 3), activation='relu', input_shape=(20, 20, 3)),
        MaxPooling2D(2, 2),
        Conv2D(64, (3, 3), activation='relu'),
        MaxPooling2D(2, 2),
        Flatten(),
        Dense(128, activation='relu'),
        Dropout(0.5),
        Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    return model

model = create_model()

# Train model
history = model.fit(train_generator, epochs=10, validation_data=(x_test, y_test))

# Evaluate the model
plt.plot(history.history['accuracy'])
plt.plot(history.history['val_accuracy'])
plt.title('Model accuracy')
plt.ylabel('Accuracy')
plt.xlabel('Epoch')
plt.legend(['Train', 'Test'], loc='upper left')
plt.show()

# Save model for future deployment
model.save('airplane_detection_model.h5')