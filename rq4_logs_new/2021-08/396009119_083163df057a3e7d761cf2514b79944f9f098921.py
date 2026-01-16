#import libraries
from flask import Flask, render_template, request
import pickle
import numpy as np

#Initialize the flask App
app = Flask(__name__)
model_Total_Count = pickle.load(open('model_Total_Count.pkl','rb'))

#default page of our web-app
@app.route('/')
def home():
    return render_template('index.html')
    
#To use the predict button in our web-app
@app.route('/predict',methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''
    int_features = [float(x) for x in request.form.values()]
    final_features = [np.array(int_features)]
    prediction_1 = model_Total_Count.predict(final_features)


    output_1 = round(prediction_1[0], 2)
    
    return render_template('index.html',prediction_text="Predicted Rented-Bike Count : {}".format(output_1))

if __name__ == "__main__":
    app.run(debug=True)