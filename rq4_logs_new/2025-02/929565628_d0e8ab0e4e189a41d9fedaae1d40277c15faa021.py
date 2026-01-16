from flask import Flask, render_template, request, jsonify
import requests

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate', methods=['POST'])
def generate():
    user_input = request.json.get("prompt", "")

    try:
        response = requests.post("http://localhost:5000/generate", json={"prompt": user_input})
        ai_response = response.json().get("response", "Error: No response from AI.")
    except Exception as e:
        ai_response = f"Error: {str(e)}"

    return jsonify({"response": ai_response})

if __name__ == '__main__':
    app.run(debug=True, port=8000)