from flask import Flask, request, render_template, jsonify
import tensorflow as tf
import numpy as np
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input, decode_predictions
from tensorflow.keras.preprocessing import image
import io

app = Flask(__name__)

# Load the MobileNetV2 model without weights (we will load custom weights)
model = MobileNetV2(weights=None, include_top=True)
model.load_weights('mobilenet_v2_weights_tf_dim_ordering_tf_kernels_1.0_224.h5')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    # Check if a file was submitted
    if 'image' not in request.files:
        return jsonify({'error': 'No image provided'})

    # Get the uploaded image
    image_file = request.files['image']

    # Check if the file has an allowed extension (e.g., jpg, jpeg, png)
    allowed_extensions = {'jpg', 'jpeg', 'png'}
    if '.' not in image_file.filename or image_file.filename.rsplit('.', 1)[1].lower() not in allowed_extensions:
        return jsonify({'error': 'Invalid file format'})

    # Load and preprocess the image for model prediction
    img = image.load_img(io.BytesIO(image_file.read()), target_size=(224, 224))
    img = image.img_to_array(img)
    img = np.expand_dims(img, axis=0)
    img = preprocess_input(img)

    # Make predictions with the model
    predictions = model.predict(img)

    # Decode the predictions to human-readable labels
    decoded_predictions = decode_predictions(predictions)

    # Format the predictions as a JSON response
    result = [{'label': label, 'probability': float(prob)} for (_, label, prob) in decoded_predictions[0]]

    return jsonify(result)

if __name__ == '__main__':
    app.run()