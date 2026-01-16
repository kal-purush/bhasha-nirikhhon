# save this as app.py
from flask import Flask, request, render_template
import pickle
import numpy as np
app = Flask(__name__)
model = pickle.load(open('model1.pkl', 'rb'))
@app.route('/login')
def home():
    return render_template("login2.html")
@app.route('/register', methods=['GET'])
def register():
    return render_template("register2.html")

@app.route('/home', methods=['GET'])
def page():
    return render_template("indexing2.html")

@app.route('/report', methods=['GET'])
def report():
    return render_template("reporting2.html")

@app.route('/predict', methods=['GET', 'POST'])
def predict():
    if request.method == 'POST':

        float_features = [float(x) for x in request.form.values()]
        features = [np.array(float_features)]
        prediction = model.predict(features)


        return render_template("predicting2.html", prediction_text="THE FOOD IS {}".format(prediction))




    else:
        return render_template("predicting2.html")


if __name__ == "__main__":
    app.run(debug=True)
