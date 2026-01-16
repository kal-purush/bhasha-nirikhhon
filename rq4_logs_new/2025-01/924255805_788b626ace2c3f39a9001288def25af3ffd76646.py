from flask import Flask, render_template, jsonify, request
import random

app = Flask(_name_)

# Predefined list of jokes
jokes = [
    "Why don't scientists trust atoms? Because they make up everything!",
    "Why was the math book sad? It had too many problems.",
    "I'm reading a book on anti-gravity. It's impossible to put down!",
    "Why did the scarecrow win an award? Because he was outstanding in his field!",
    "I told my wife she was drawing her eyebrows too high. She looked surprised."
]

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/get_joke", methods=["GET"])
def get_joke():
    joke = random.choice(jokes)
    return jsonify({"joke": joke})

if _name_ == "_main_":
    app.run(debug=True)
  