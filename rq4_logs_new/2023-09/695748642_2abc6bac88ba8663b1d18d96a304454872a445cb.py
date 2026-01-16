from flask import Flask, request, jsonify,render_template
import numpy as np
import joblib

app = Flask(__name__)

# Load your machine learning model here
model = joblib.load('prediction_model.pkl')

# Load your label encoder here
le = joblib.load('label_encoder.pkl')  # Replace 'label_encoder.pkl' with your actual label encoder file


@app.route('/')
def start():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get the JSON data from the request
        data = request.json

        # Extract the "features" key containing the list of binary values
        features = data.get("features")

        if features is not None and len(features) == 132:
            # Convert the list to a NumPy array
            input_data = np.array(features)

            # Make a prediction using your model
            prediction = model.predict([input_data])

            # Use the label encoder to transform the prediction back to its original label
            predicted_label = le.inverse_transform(prediction)

            # You can return the result as a string
            result = predicted_label[0]

            return jsonify({"prediction": result})

        else:
            return jsonify({"error": "Invalid input data"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)