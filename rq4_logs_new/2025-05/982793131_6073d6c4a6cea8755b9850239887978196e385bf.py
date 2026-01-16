from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import csv
import io
import os
import json
import pickle
import numpy as np
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize the database
def init_db():
    conn = sqlite3.connect('feedback.db')
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comment TEXT NOT NULL,
        department TEXT,
        sentiment TEXT NOT NULL,
        score REAL NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()

# Train or load the sentiment analysis model
def get_sentiment_model():
    model_path = 'sentiment_model.pkl'
    vectorizer_path = 'vectorizer.pkl'
    
    # If model already exists, load it
    if os.path.exists(model_path) and os.path.exists(vectorizer_path):
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        with open(vectorizer_path, 'rb') as f:
            vectorizer = pickle.load(f)
        return vectorizer, model
    
    # Otherwise, train a simple model with some example data
    # This is a very basic dataset - in a real application, you'd use more data
    training_data = [
        # Positive examples
        "The professor was very helpful and engaging.",
        "I really enjoyed the class today.",
        "The course materials were excellent and well-organized.",
        "The instructor explained the concepts clearly.",
        "The feedback from the teacher was constructive and useful.",
        "I appreciate how responsive the department is to student concerns.",
        "The new facilities are a great improvement.",
        "The online resources provided were very helpful for studying.",
        
        # Negative examples
        "The lecture was boring and hard to follow.",
        "The assignment instructions were unclear.",
        "The classroom was too crowded and noisy.",
        "The professor didn't answer my questions properly.",
        "The course materials were outdated.",
        "The grading system is unfair.",
        "The department is disorganized.",
        "The feedback was not helpful at all.",
        
        # Neutral examples
        "The class starts at 9 AM.",
        "The textbook has 12 chapters.",
        "The exam will cover all material from the semester.",
        "The assignment is due next Friday.",
        "The department office is located in building B.",
        "The course has three sections.",
        "The lecture slides are available online.",
        "The professor's office hours are on Tuesdays."
    ]
    
    # Labels: 1 for positive, 0 for neutral, -1 for negative
    labels = [1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0]
    
    # Create and train the model
    vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
    model = MultinomialNB()
    
    X = vectorizer.fit_transform(training_data)
    model.fit(X, labels)
    
    # Save the model for future use
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    with open(vectorizer_path, 'wb') as f:
        pickle.dump(vectorizer, f)
    
    return vectorizer, model

# Get the sentiment model
vectorizer, model = get_sentiment_model()

# Analyze sentiment of a comment using ML model
def analyze_sentiment(comment):
    if not comment or comment.isspace():
        return {"sentiment": "neutral", "score": 0.0}
    
    # Transform the comment using the vectorizer
    X = vectorizer.transform([comment])
    
    # Predict the sentiment class (-1, 0, 1)
    sentiment_class = model.predict(X)[0]
    
    # Get probability scores
    proba = model.predict_proba(X)[0]
    
    # Map the class to sentiment label
    if sentiment_class == 1:
        sentiment = "positive"
        # Use the probability of the positive class as the score
        score = proba[np.where(model.classes_ == 1)[0][0]]
    elif sentiment_class == -1:
        sentiment = "negative"
        # Use the negative of the probability of the negative class as the score
        score = -proba[np.where(model.classes_ == -1)[0][0]]
    else:
        sentiment = "neutral"
        score = 0.0
    
    return {"sentiment": sentiment, "score": float(score)}

# Route to analyze a single comment
@app.route('/api/analyze', methods=['POST'])
def analyze_comment():
    data = request.json
    comment = data.get('comment', '')
    department = data.get('department', '')
    
    result = analyze_sentiment(comment)
    
    # Store in database
    conn = sqlite3.connect('feedback.db')
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO feedback (comment, department, sentiment, score) VALUES (?, ?, ?, ?)',
        (comment, department, result['sentiment'], result['score'])
    )
    conn.commit()
    
    # Get the ID of the inserted row
    feedback_id = cursor.lastrowid
    conn.close()
    
    return jsonify({
        "id": feedback_id,
        "comment": comment,
        "department": department,
        "sentiment": result['sentiment'],
        "score": result['score']
    })

# Route to analyze multiple comments from CSV
@app.route('/api/analyze-csv', methods=['POST'])
def analyze_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if file:
        # Save the file temporarily
        file_path = 'temp_csv.csv'
        file.save(file_path)
        
        try:
            # Read the CSV file using built-in csv module
            results = []
            conn = sqlite3.connect('feedback.db')
            cursor = conn.cursor()
            
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                
                # Check if required columns exist
                if 'comment' not in csv_reader.fieldnames:
                    return jsonify({"error": "CSV must contain a 'comment' column"}), 400
                
                # Process each row
                for row in csv_reader:
                    comment = row.get('comment', '').strip()
                    department = row.get('department', '').strip() if 'department' in csv_reader.fieldnames else ''
                    
                    if not comment:
                        continue
                    
                    result = analyze_sentiment(comment)
                    
                    # Store in database
                    cursor.execute(
                        'INSERT INTO feedback (comment, department, sentiment, score) VALUES (?, ?, ?, ?)',
                        (comment, department, result['sentiment'], result['score'])
                    )
                    
                    # Get the ID of the inserted row
                    feedback_id = cursor.lastrowid
                    
                    results.append({
                        "id": feedback_id,
                        "comment": comment,
                        "department": department,
                        "sentiment": result['sentiment'],
                        "score": result['score']
                    })
            
            conn.commit()
            conn.close()
            
            # Remove temporary file
            os.remove(file_path)
            
            return jsonify({"results": results})
            
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    return jsonify({"error": "Failed to process file"}), 500

# Route to get all feedback
@app.route('/api/feedback', methods=['GET'])
def get_feedback():
    conn = sqlite3.connect('feedback.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM feedback ORDER BY timestamp DESC')
    rows = cursor.fetchall()
    conn.close()
    
    feedback = []
    for row in rows:
        feedback.append({
            "id": row['id'],
            "comment": row['comment'],
            "department": row['department'],
            "sentiment": row['sentiment'],
            "score": row['score'],
            "timestamp": row['timestamp']
        })
    
    return jsonify({"feedback": feedback})

# Route to export feedback as CSV
@app.route('/api/export-csv', methods=['GET'])
def export_csv():
    conn = sqlite3.connect('feedback.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM feedback ORDER BY timestamp DESC')
    rows = cursor.fetchall()
    conn.close()
    
    # Create CSV in memory using StringIO and csv module
    output = io.StringIO()
    csv_writer = csv.writer(output)
    
    # Write header
    csv_writer.writerow(['id', 'comment', 'department', 'sentiment', 'score', 'timestamp'])
    
    # Write data rows
    for row in rows:
        csv_writer.writerow([
            row['id'],
            row['comment'],
            row['department'],
            row['sentiment'],
            row['score'],
            row['timestamp']
        ])
    
    csv_data = output.getvalue()
    
    return jsonify({"csv": csv_data})

# Initialize database when the app starts
init_db()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)