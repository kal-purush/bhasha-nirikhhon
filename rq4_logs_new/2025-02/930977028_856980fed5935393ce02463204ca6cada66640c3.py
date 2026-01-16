from flask import Flask, request, jsonify, render_template
import json

app = Flask(__name__)
JSON_FILE = "data.json"

app = Flask(__name__, static_folder="static", template_folder="templates")

@app.route("/")
def home():
    return render_template("map.html")  # Make sure 'index.html' is in the 'templates' folder

# Load JSON data
def load_data():
    with open(JSON_FILE, "r") as file:
        return json.load(file)

# Save JSON data
def save_data(data):
    with open(JSON_FILE, "w") as file:
        json.dump(data, file, indent=4)

# Read JSON
@app.route("/data", methods=["GET"])
def get_data():
    return jsonify(load_data())

# Add new entry
@app.route("/data", methods=["POST"])
def add_data():
    new_entry = request.json
    data = load_data()
    data.append(new_entry)  # Modify JSON here
    save_data(data)
    return jsonify({"message": "Data added", "data": new_entry})

if __name__ == "__main__":
    app.run(debug=True)