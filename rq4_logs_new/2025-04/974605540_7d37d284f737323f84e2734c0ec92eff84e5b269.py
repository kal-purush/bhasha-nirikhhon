from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import openai

app = Flask(__name__, static_folder="static")
CORS(app)

openai.api_key = "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"  
openai.base_url = "https://api.groq.com/openai/v1/" 

@app.route("/")
def serve_index():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory(app.static_folder, path)

@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json()
    user_input = data['message']
    response = generate_hearttalk_response(user_input)
    return jsonify({"response": response})

conversation_history = [
    {"role": "system", "content": "You are HeartTalk, a kind and helpful medical assistant chatbot. Always answer carefully and professionally."}
]

def generate_hearttalk_response(user_input):
    conversation_history.append({"role": "user", "content": user_input})

    chat_completion = openai.chat.completions.create(
        model="llama3-8b-8192",
        messages=conversation_history,
        temperature=0.7,
        max_tokens=200,
        top_p=0.9
    )

    reply = chat_completion.choices[0].message.content.strip()

    conversation_history.append({"role": "assistant", "content": reply})

    return reply

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)