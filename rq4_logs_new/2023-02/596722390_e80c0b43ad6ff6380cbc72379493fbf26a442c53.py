import numpy as np
import pickle
import pandas as pd
import uvicorn
from fastapi import FastAPI
from diabetes_data import Diabetes
from sklearn.preprocessing import StandardScaler


app = FastAPI()

pickle_in = open("svc_model.pkl","rb")
classifier=pickle.load(pickle_in)

sc = StandardScaler()

@app.post('/predict_diabetes')
def predict_diabetes(data:Diabetes):
    data = data.dict()
    Pregnancies = data['Pregnancies']
    Glucose	= data['Glucose']
    BloodPressure = data['BloodPressure']
    SkinThickness = data['SkinThickness']
    Insulin	= data['Insulin']
    BMI	= data['BMI']
    DiabetesPedigreeFunction = data['DiabetesPedigreeFunction']
    Age = data['Age']
    
    data = sc.fit_transform([[Pregnancies,Glucose,BloodPressure,SkinThickness,Insulin,BMI,DiabetesPedigreeFunction,Age]])
    
    prediction = classifier.predict(data)
    
    if(prediction[0]>0.5):
        prediction="Diabetic"
    else:
        prediction="Not Diabetic"
    return {'prediction': prediction}




if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
        
        
##uvicorn diabetes:app --reload