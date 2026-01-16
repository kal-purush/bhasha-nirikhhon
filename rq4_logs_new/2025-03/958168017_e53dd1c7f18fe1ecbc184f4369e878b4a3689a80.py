import os
import logging
import numpy as np
import cv2
import pickle
import base64
from flask import Flask, render_template, request, jsonify, Response, session

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "default_secret_key")

# Global variables
training_data = {
    'rock': [],
    'paper': [],
    'scissors': []
}

# Load the model if it exists
model = None
try:
    with open('hand_gesture_model.pkl', 'rb') as f:
        model = pickle.load(f)
    logger.debug("Model loaded successfully")
except FileNotFoundError:
    logger.debug("No model found, training will be required")

# Game state
game_state = {
    'player_score': 0,
    'computer_score': 0,
    'rounds': 0,
    'result': ''
}

def reset_game_state():
    global game_state
    game_state = {
        'player_score': 0,
        'computer_score': 0,
        'rounds': 0,
        'result': ''
    }

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/training')
def training():
    return render_template('training.html')

@app.route('/game')
def game():
    # Reset the game state when starting a new game
    reset_game_state()
    return render_template('game.html')

@app.route('/capture_training_image', methods=['POST'])
def capture_training_image():
    data = request.json
    image_data = data.get('image')
    gesture = data.get('gesture')
    
    if not image_data or not gesture:
        return jsonify({'success': False, 'error': 'Missing image or gesture data'})
    
    # Decode base64 image
    try:
        # Remove the base64 prefix if present
        if 'base64,' in image_data:
            image_data = image_data.split('base64,')[1]
        
        image_bytes = base64.b64decode(image_data)
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        # Process the image (convert to grayscale, resize, etc.)
        processed_img = preprocess_image(img)
        
        # Extract features
        features = extract_features(processed_img)
        
        # Add to training data
        training_data[gesture].append(features)
        
        # Get current counts
        counts = {k: len(v) for k, v in training_data.items()}
        
        return jsonify({
            'success': True, 
            'message': f'Captured {gesture} image', 
            'counts': counts
        })
    except Exception as e:
        logger.error(f"Error processing training image: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/train_model', methods=['POST'])
def train_model():
    global model
    
    # Check if we have enough training data
    min_samples = min(len(training_data['rock']), 
                      len(training_data['paper']), 
                      len(training_data['scissors']))
    
    if min_samples < 10:
        return jsonify({
            'success': False, 
            'error': f'Not enough training data. Need at least 10 images per gesture. Currently have: Rock: {len(training_data["rock"])}, Paper: {len(training_data["paper"])}, Scissors: {len(training_data["scissors"])}'
        })
    
    try:
        from sklearn.ensemble import RandomForestClassifier
        
        # Prepare training data
        X = np.vstack([
            np.array(training_data['rock']),
            np.array(training_data['paper']),
            np.array(training_data['scissors'])
        ])
        
        y = np.hstack([
            np.full(len(training_data['rock']), 'rock'),
            np.full(len(training_data['paper']), 'paper'),
            np.full(len(training_data['scissors']), 'scissors')
        ])
        
        # Train a random forest classifier
        model = RandomForestClassifier(n_estimators=50)
        model.fit(X, y)
        
        # Save the model
        with open('hand_gesture_model.pkl', 'wb') as f:
            pickle.dump(model, f)
        
        return jsonify({'success': True, 'message': 'Model trained successfully!'})
    except Exception as e:
        logger.error(f"Error training model: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/predict_gesture', methods=['POST'])
def predict_gesture():
    global model, game_state
    
    if model is None:
        return jsonify({'success': False, 'error': 'Model not trained yet'})
    
    data = request.json
    image_data = data.get('image')
    
    if not image_data:
        return jsonify({'success': False, 'error': 'Missing image data'})
    
    try:
        # Decode base64 image
        if 'base64,' in image_data:
            image_data = image_data.split('base64,')[1]
        
        image_bytes = base64.b64decode(image_data)
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        # Process the image
        processed_img = preprocess_image(img)
        
        # Extract features
        features = extract_features(processed_img)
        
        # Make prediction
        prediction = model.predict([features])[0]
        
        # Generate computer choice
        computer_choice = np.random.choice(['rock', 'paper', 'scissors'])
        
        # Determine winner
        result = determine_winner(prediction, computer_choice)
        
        # Update game state
        if result == 'win':
            game_state['player_score'] += 1
        elif result == 'lose':
            game_state['computer_score'] += 1
        
        game_state['rounds'] += 1
        game_state['result'] = result
        
        return jsonify({
            'success': True,
            'player_gesture': prediction,
            'computer_choice': computer_choice,
            'result': result,
            'game_state': game_state
        })
    except Exception as e:
        logger.error(f"Error predicting gesture: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

def preprocess_image(img):
    """Preprocess image for feature extraction"""
    # Convert to grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Apply Gaussian blur to reduce noise
    blurred = cv2.GaussianBlur(gray, (7, 7), 0)
    
    # Apply threshold to create binary image
    _, threshold = cv2.threshold(blurred, 127, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    
    # Resize to a standard size
    resized = cv2.resize(threshold, (64, 64))
    
    return resized

def extract_features(img):
    """Extract features from processed image"""
    # For a simple approach, we'll just flatten the image and use pixel values as features
    features = img.flatten() / 255.0  # Normalize pixel values
    
    # Calculate HOG features (optional, for better accuracy)
    # This is a simplified version, for real applications consider using proper HOG implementation
    
    return features

def determine_winner(player, computer):
    """Determine winner of Rock Paper Scissors round"""
    if player == computer:
        return 'tie'
    elif (player == 'rock' and computer == 'scissors') or \
         (player == 'scissors' and computer == 'paper') or \
         (player == 'paper' and computer == 'rock'):
        return 'win'
    else:
        return 'lose'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)