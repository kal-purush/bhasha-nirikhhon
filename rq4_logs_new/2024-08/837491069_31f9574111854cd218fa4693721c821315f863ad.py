from flask import Flask, request, render_template
import pandas as pd
import joblib
import os

app = Flask(__name__)

# Load the AI model and scaler
model = joblib.load('credit_model.pkl')
scaler = joblib.load('scaler.pkl')

# Function to preprocess the data and predict credit scores
def predict_credit_scores(file_path):
    # Load data from CSV file
    data = pd.read_csv(file_path)
    
    # Extract features
    X = data[['age', 'income', 'loan_amount', 'open_accounts', 'credit_history_length', 'defaulted_before']]
    
    # Standardize features
    X_scaled = scaler.transform(X)
    
    # Predict credit scores
    credit_scores = model.predict(X_scaled)
    
    # Add predictions to the original data
    data['predicted_credit_score'] = credit_scores
    
    return data

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    if 'file' not in request.files:
        return "No file part"
    
    file = request.files['file']
    
    if file.filename == '':
        return "No selected file"
    
    if file:
        file_path = os.path.join('uploads', file.filename)
        file.save(file_path)
        predictions = predict_credit_scores(file_path)
        predictions_html = predictions.to_html()
        return render_template('index.html', predictions=predictions_html)

if __name__ == '__main__':
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    app.run(debug=True)