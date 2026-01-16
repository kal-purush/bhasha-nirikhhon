from flask import Flask, request, jsonify, render_template
import pickle
import re
import string
from sklearn.feature_extraction.text import TfidfVectorizer

app = Flask(__name__)

# Load pre-trained model and vectorizer
with open('model.pkl', 'rb') as model_file:
    model = pickle.load(model_file)

with open('vectorizer.pkl', 'rb') as vec_file:
    vectorizer = pickle.load(vec_file)

# Preprocessing function
def preprocess_text(text):
    text = text.lower()
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r"https?://\S+|www\.\S+", '', text)
    text = re.sub(r'[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub(r'\w*\d\w*', '', text)
    return text

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    input_data = request.form['text']
    # Preprocess input data
    processed_data = preprocess_text(input_data)
    # Vectorize input text
    vectorized_data = vectorizer.transform([processed_data])
    # Predict using the ML model
    prediction = model.predict(vectorized_data)[0]
    probability = model.predict_proba(vectorized_data)[0]
    accuracy = round(max(probability) * 100, 2)

    # Return response
    return jsonify({
        "text": input_data,
        "prediction": "Fake News" if prediction == 0 else "True News",
        "accuracy": f"{accuracy}%"
    })

if __name__ == '__main__':
    app.run(debug=True)