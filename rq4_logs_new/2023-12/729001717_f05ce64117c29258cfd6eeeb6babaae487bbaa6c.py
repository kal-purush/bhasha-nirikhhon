from flask import Flask, request, Response
from joblib import load
import numpy as np

my_lr_mod = load("Model/irispred.joblib")
# Initializing
app = Flask(__name__)


@app.route("/predictions", methods=['POST', 'GET'])
def predictions():
    data = request.json

    user_sent_this_data = data.get('mydata')
    user_number = np.array(user_sent_this_data).reshape(1, -1)

    model_prediction = my_lr_mod.predict(user_number)

    # returning the response
    return Response(str(model_prediction))


if __name__ == '__main__':
    app.run(debug=True)