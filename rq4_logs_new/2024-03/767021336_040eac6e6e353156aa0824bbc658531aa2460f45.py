from flask import Flask,request,render_template,url_for
import numpy as np
from sklearn.preprocessing import StandardScaler
import joblib as joblib
import os

model=joblib.load('model.pkl')
scaler=joblib.load('model.sav')

app =Flask(__name__)



@app.route('/')
def index():
    return render_template('index.html')

@app.route('/',methods=['GET','POST'])
def home():
    if request.method =='POST':
        sl=request.form['cons.price.idx']
        sw = request.form['euribor3m']
        pl = request.form['pdays']
        pw = request.form['age']
        data = np.array([[sl, sw, pl, pw]])
        x = scaler.transform(data)
        print(x)
        prediction = model.predict(x)
        print(prediction)
    return render_template('index.html',prediction=prediction[0])


if __name__ == '__main__':
    app.run(debug=True)