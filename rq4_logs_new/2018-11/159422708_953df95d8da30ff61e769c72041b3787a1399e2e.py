from flask import Flask,render_template,jsonify
import pandas as pd

app=Flask(__name__)
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def csvdata():
    df=pd.read_csv('data/data.csv')
    return jsonify(df.to_dict(orient="files"))  


if __name__="__main__":
    app.run(debug=True)