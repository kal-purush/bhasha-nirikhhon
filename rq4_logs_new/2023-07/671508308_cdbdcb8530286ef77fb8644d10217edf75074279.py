from flask import Flask, request, send_file, jsonify
from flask_cors import CORS, cross_origin
import tensorflow as tf
from tensorflow.keras.models import load_model
import pandas as pd
import numpy as np
import json

import env

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.secret_key = "chronicare123"
cors = CORS(app, resources={r"/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'


def composeReply(status, message, payload = None):
    reply = {}
    reply["SENDER"] = "CHRONICARE AI"
    reply["STATUS"] = status
    reply["MESSAGE"] = message
    reply["PAYLOAD"] = payload
    return jsonify(reply)


@app.route("/diabetes", methods=['POST'])
def diabetes():
    age = request.form.get("age")
    gender = request.form.get("gender")
    polyuria = request.form.get("polyuria")
    polydipsia = request.form.get("polydipsia")
    sudden_weight_loss = request.form.get("sudden_weight_loss")
    polyphagia = request.form.get("polyphagia")
    delayed_healing = request.form.get("delayed_healing")
    obesity = request.form.get("obesity")
    
    #model = load_model('diabetes/diabetes.h5')
    model = load_model(env.fullPath + '\\diabetes\\diabetes.h5')
    #model = load_model(env.fullPath + '/ml/ml.h5')
    
    new_data_dict = {
        'age':                  [int(age)],
        'gender':               [int(gender)],
        'polyuria':             [int(polyuria)],
        'polydipsia':           [int(polydipsia)],
        'sudden_weight_loss':   [int(sudden_weight_loss)],
        'polyphagia':           [int(polyphagia)],
        'delayed_healing':      [int(delayed_healing)],
        'obesity':              [int(obesity)]
    }

    data = json.load(open('diabetes/diabetes.json'))

    new_data = pd.DataFrame(new_data_dict)
    new_data['age'] = (new_data['age'] - data["age_min"]) / (data["age_max"] - data["age_min"])

    predictions = model.predict(new_data)
    print("============================================================")
    finalPredict = predictions[0].tolist()[0]
    print(finalPredict)
    
    probabilities_positive_class = predictions[:, 0]
    probabilities_negative_class = 1 - probabilities_positive_class
    confident_percent_positive_class = probabilities_positive_class * 100
    confident_percent_negative_class = probabilities_negative_class * 100

    confident_p = round(confident_percent_positive_class.tolist()[0], 2)
    confident_n = round(confident_percent_negative_class.tolist()[0], 2)

    returnData = {
        "PREDICTION" : finalPredict,
        "CONFIDENT" : confident_p if finalPredict > 0.5 else confident_n,
        "CONFIDENT_POSITIVE" : confident_p,
        "CONFIDENT_NEGATIVE" : confident_n,
    }

    return composeReply("SUCCESS", "Prediksi", returnData)


if __name__ == '__main__':
    app.run(host = env.runHost, port = env.runPort, debug = env.runDebug)