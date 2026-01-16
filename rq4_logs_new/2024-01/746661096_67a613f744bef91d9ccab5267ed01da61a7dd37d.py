import tensorflow as tf
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input, decode_predictions
from tensorflow.keras.preprocessing import image
from PIL import Image
import numpy as np

# Load the pre-trained MobileNetV2 model
model = MobileNetV2(weights='imagenet')

def classify_image(img_path):
    # Load and preprocess the image
    img = Image.open(img_path)
    img = img.resize((224, 224))  # Resize image to 224x224 as expected by MobileNetV2
    img_array = image.img_to_array(img)
    img_array = np.expand_dims(img_array, axis=0)
    img_array = preprocess_input(img_array)

    # Make predictions
    predictions = model.predict(img_array)
    
    # Decode and print predictions
    print('Predictions:')
    for i, pred in enumerate(decode_predictions(predictions, top=3)[0]):
        print(f"{i + 1}: {pred[1]} ({pred[2] * 100:.2f}%)")

# Example usage
classify_image('path_to_your_image.jpg')