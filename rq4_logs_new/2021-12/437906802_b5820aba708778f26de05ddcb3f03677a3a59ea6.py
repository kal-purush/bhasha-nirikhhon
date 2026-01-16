from flask import Flask, render_template, request, jsonify
import pickle
import numpy as np

model = pickle.load(open('GradSchool.pkl', 'rb'))

app = Flask(__name__)


@app.route('/')
def hello_world():
    return render_template('home.html')


@app.route('/', methods=['POST'])
def predict():
    d1 = request.form['gre']
    d2 = request.form['tfl']
    d3 = request.form['lor']
    d4 = request.form['sop']
    d5 = request.form['cgpa']
    d6 = request.form['ra']
    arr = np.array([[d1, d2, d3, d4, d5, d6]])
    output = model.predict(arr).astype(int)[0]
    return render_template('final.html', result=output)

if __name__ == "__main__":
    app.run(debug=True)