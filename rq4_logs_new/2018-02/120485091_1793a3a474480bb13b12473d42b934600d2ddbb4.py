from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/')
def index():
    return 'Welcome!'

@app.route('/api/info', methods=['GET'])
def info():
    return jsonify({ 'name': 'flask api demo', 'version': '1.0.0' })

if __name__ == "__main__":
    app.run(debug=True)