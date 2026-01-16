from flask import Flask, request, jsonify, send_from_directory, make_response
import psycopg2
import os
import uuid
from my_clasifier import category_clasification
from my_generator import generate_answer
from transformers import TFAutoModelForSequenceClassification, AutoTokenizer
import joblib
from tensorflow.keras.models import load_model



app = Flask(__name__)


# Load the model and tokenizer
model_path = 'model_lstm_v_1.0.h5'


# Load the tokenizer
tokenizer = joblib.load('tokenizer.pkl')

# Load the label encoder
label_encoder = joblib.load('label_encoder.pkl')


# Load the model from the .h5 file
model = load_model(model_path)


# Database connection
def get_db_connection():
    #conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    conn = psycopg2.connect('postgres://postgres:password@localhost:5432/postgres')
    return conn

# Create table if not exists
def create_table():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS qa_logs
                    (id SERIAL PRIMARY KEY,
                    summary TEXT,
                    question TEXT,
                    answer TEXT,
                    environment TEXT,
                    app_version TEXT,
                    model TEXT,
                    model_version TEXT,
                    trace_id UUID,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    cur.close()
    conn.close()

# Call create_table when the app starts
create_table()



# Serve static/index.html on root route
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

# Handle POST request on /ask
@app.route('/ask', methods=['POST'])
def ask():
    data = request.json
    summary = data.get('summary', '')
    question = data.get('question', '')
    
    category = category_clasification(model, tokenizer, label_encoder, summary, question)



    answer =  generate_answer(summary,question,category)

    
    # Get trace ID from headers or generate a new one
    trace_id = request.headers.get('Trace-ID', str(uuid.uuid4()))
    
    # Save to database
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''INSERT INTO qa_logs 
                (summary, question, answer, environment, app_version, model, model_version, trace_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                (summary, question, answer, 
                os.getenv('FLASK_ENV', 'development'),  # environment
                '1.0',  # app_version (you can set this to your actual version)
                'My first mod',  # model (replace with your actual model)
                '1.0',  # model_version (replace with your actual model version)
                trace_id))
    conn.commit()
    cur.close()
    conn.close()
    
    return jsonify({"answer": answer, "trace_id": str(trace_id)})
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
