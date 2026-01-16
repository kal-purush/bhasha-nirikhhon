# app.py - Main Flask application
import os
import json
import numpy as np
import pickle
import nltk
from nltk.stem import WordNetLemmatizer
from keras.api.models import load_model
from flask import Flask, request, jsonify
from flask_cors import CORS

# Download nltk data if not already downloaded
nltk.download('punkt', quiet=True)
nltk.download('wordnet', quiet=True)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

# Load trained model and data
try:
    model = load_model('chatbot_model.h5')
    with open('intents.json', 'r') as file:
        intents = json.load(file)
    words = pickle.load(open('words.pkl', 'rb'))
    classes = pickle.load(open('classes.pkl', 'rb'))
    print("Model and data loaded successfully!")
except Exception as e:
    print(f"Error loading model or data: {e}")
    print("Running training script...")
    os.system('python train_chatbot.py')
    # Try loading again after training
    model = load_model('chatbot_model.h5')
    with open('intents.json', 'r') as file:
        intents = json.load(file)
    words = pickle.load(open('words.pkl', 'rb'))
    classes = pickle.load(open('classes.pkl', 'rb'))
    print("Model and data loaded successfully after training!")


def clean_up_sentence(sentence):
    """Tokenize and lemmatize the sentence"""
    sentence_words = nltk.word_tokenize(sentence)
    sentence_words = [lemmatizer.lemmatize(word.lower()) for word in sentence_words]
    return sentence_words


def bag_of_words(sentence):
    """Convert a sentence to a bag of words array"""
    sentence_words = clean_up_sentence(sentence)
    bag = [0] * len(words)
    for w in sentence_words:
        for i, word in enumerate(words):
            if word == w:
                bag[i] = 1
    return np.array(bag)


def predict_class(sentence):
    """Predict the class (intent) of a sentence"""
    bow = bag_of_words(sentence)
    res = model.predict(np.array([bow]))[0]
    
    # Filter out predictions below a threshold
    ERROR_THRESHOLD = 0.25
    results = [[i, r] for i, r in enumerate(res) if r > ERROR_THRESHOLD]
    
    # Sort by strength of probability
    results.sort(key=lambda x: x[1], reverse=True)
    return_list = []
    for r in results:
        return_list.append({"intent": classes[r[0]], "probability": str(r[1])})
    
    return return_list


def get_response(intents_list, intents_json):
    """Get a response from the model"""
    if not intents_list:
        return "I'm not sure what you mean. Could you rephrase that?"
    
    tag = intents_list[0]['intent']
    list_of_intents = intents_json['intents']
    
    for i in list_of_intents:
        if i['tag'] == tag:
            # Get a random response from the intent
            result = np.random.choice(i['responses'])
            break
    else:
        result = "I'm not sure how to respond to that."
    
    return result


@app.route('/api/message', methods=['POST'])
def process_message():
    data = request.json
    message = data.get('message', '')
    
    if not message:
        return jsonify({"error": "No message provided"}), 400
    
    # Get prediction and response
    ints = predict_class(message)
    response = get_response(ints, intents)
    
    return jsonify({
        "response": response,
        "intents": ints
    })


@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "model_loaded": model is not None})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)