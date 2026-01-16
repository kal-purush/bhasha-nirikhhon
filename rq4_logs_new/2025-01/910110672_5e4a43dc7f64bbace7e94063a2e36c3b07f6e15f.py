from flask import Flask, request, jsonify
from flask_cors import CORS


PORT_NUMBER = 3001

app = Flask(__name__)
CORS(app)


features = []
alternatives = []
matrix = []
weights = []



@app.post('/sendData')
def get_data():
    global features, alternatives, matrix, weights

    data = request.get_json()
    features = data.get('features')
    alternatives = data.get('alternatives')
    matrix = data.get('matrix')
    weights = data.get('weights')

    print(f"features: {features}")
    print(f"alternatives: {alternatives}")
    print(f"matrix: {matrix}")
    print(f"weights: {weights}")
    
    return jsonify({'status': 'success'})


if __name__=='__main__':
    app.run(debug=True, port=PORT_NUMBER)