from flask import Flask, jsonify, request
from flask_cors import CORS
from openai import OpenAI
import os
from dotenv import load_dotenv, find_dotenv


_ = load_dotenv(find_dotenv())
client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

def generate_response(conversation, model="gpt-4-0125-preview"):
    response = client.chat.completions.create(
        model=model,
        messages=conversation
    )
    return response.choices[0].message.content

app = Flask(__name__)
CORS(app)

@app.route('/api/submit', methods=['POST'])
def handleSubmit():
    data = request.json
    print(data)

    messages = [{'role': 'system', 'content': 'You are a chat bot for a Chevron Information Website.'}]
    for msg in data:
        # Assuming each message in 'data' has 'sender' and 'text' keys
        role = 'user' if msg['sender'] == 'user' else 'assistant'
        messages.append({'role': role, 'content': msg['text']})

    # Generate a response using the OpenAI API
    bot_response = generate_response(messages)

    print(bot_response)

    # Return the response
    return jsonify({'status': 'Success', 'data': data})

if __name__ == '__main__':
    app.run(debug=True)