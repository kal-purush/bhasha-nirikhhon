from flask import Flask, jsonify, request
#import pandas as pd
import os
#from sklearn.externals import joblib
#from sklearn.linear_model import LinearRegression
import h5py
import keras
import numpy as np
import cv2

from app.settings import *  #loads in the config settings variable


app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = app_settings['UPLOAD_FOLDER']

#def init():
global model,graph, dog_names
# load the pre-trained Keras model
model = keras.models.load_model('app/dog_classifier.h5')
model._make_predict_function()
#h5f = h5py.File('dog_labels.h5', 'r')
#dog_names = h5f['dogs'][:]
dog_names = np.load('app/dog_labels.npy')
#h5f.close()

@app.route("/dog_classifier_api/predict", methods=['POST'])
def predict():
    if request.method == 'POST':
#        try:
            #data = request.get_json()
            #years_of_experience = float(data["yearsOfExperience"])

            #lin_reg = joblib.load("./linear_regression_model.pkl")
#            if not 'file' in request.files:
#                return jsonify({'error': 'no file'})  #, 400
            # Image info
            img_file = request.files.get('file')
            img_name = img_file.filename
            mimetype = img_file.content_type
            # Return an error if not a valid mimetype
#            if not mimetype in valid_mimetypes:
#                return jsonify({'error': 'bad-type'})
            # Write image to static directory and do the hot dog check
            img_file.save(os.path.join(app.config['UPLOAD_FOLDER'], img_name))
            image_resize = 150
            X = []
            img = cv2.imread(os.path.join(app.config['UPLOAD_FOLDER'], img_name))
            X.append(cv2.resize(img, (image_resize, image_resize)))
            X = np.array(X, np.float32) / 255.
            prediction = model.predict(X, verbose=0)

            # Get top n=3 predictions
            n=3
            flat_indices = np.argpartition(-prediction, n-1)[0][:n]
            breed = dog_names[flat_indices][:n].tolist()
            print(flat_indices)
            print(breed)
            #flat_indices = numpy.argpartition(prediction.ravel(), n-1)[:n]
            # for 2D array  row_indices, col_indices = numpy.unravel_index(flat_indices, prediction.shape)


#            hot_dog_conf = rekognizer.get_confidence(img_name)
            # Delete image when done with analysis
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], img_name))
#            is_hot_dog = 'false' if hot_dog_conf == 0 else 'true'
#            return_packet = {
#                'is_hot_dog': is_hot_dog,
#                'confidence': hot_dog_conf
#            }
#            return jsonify(return_packet)
            #breed = prediction[0][0]
            #breed = 'Dog'
#            return_packet = {
#                'is_hot_dog': is_hot_dog,
#                'confidence': hot_dog_conf
#            }
#            return jsonify(return_packet)
            return_packet = {
                'dog': breed,
                'file': img_name,
                'type': mimetype
            }
            return jsonify(return_packet)

#        except ValueError:
#            return jsonify("Please enter a number.")



@app.route("/retrain", methods=['POST'])
def retrain():
    if request.method == 'POST':
        data = request.get_json()

        try:
            training_set = joblib.load("./training_data.pkl")
            training_labels = joblib.load("./training_labels.pkl")

            df = pd.read_json(data)

            df_training_set = df.drop(["Salary"], axis=1)
            df_training_labels = df["Salary"]

            df_training_set = pd.concat([training_set, df_training_set])
            df_training_labels = pd.concat([training_labels, df_training_labels])

            new_lin_reg = LinearRegression()
            new_lin_reg.fit(df_training_set, df_training_labels)

            os.remove("./linear_regression_model.pkl")
            os.remove("./training_data.pkl")
            os.remove("./training_labels.pkl")

            joblib.dump(new_lin_reg, "linear_regression_model.pkl")
            joblib.dump(df_training_set, "training_data.pkl")
            joblib.dump(df_training_labels, "training_labels.pkl")

            lin_reg = joblib.load("./linear_regression_model.pkl")
        except ValueError as e:
            return jsonify("Error when retraining - {}".format(e))

        return jsonify("Retrained model successfully.")


@app.route("/currentDetails", methods=['GET'])
def current_details():
    if request.method == 'GET':
        try:
            lr = joblib.load("./linear_regression_model.pkl")
            training_set = joblib.load("./training_data.pkl")
            labels = joblib.load("./training_labels.pkl")

            return jsonify({"score": lr.score(training_set, labels),
                            "coefficients": lr.coef_.tolist(), "intercepts": lr.intercept_})
        except (ValueError, TypeError) as e:
            return jsonify("Error when getting details - {}".format(e))


if __name__ == '__main__':
    init()
    app.run(debug=True)