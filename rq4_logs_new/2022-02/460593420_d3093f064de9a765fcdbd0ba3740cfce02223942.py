from flask import Flask,request, url_for, redirect, render_template, jsonify

#import pandas as pd
import pickle
#import numpy as np
#import numpy as np 
import string
#from nltk.corpus import stopwords
#import pandas as pd 
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.tree import DecisionTreeClassifier
from sklearn.feature_extraction.text import TfidfTransformer,TfidfVectorizer
from sklearn.pipeline import Pipeline
import pickle




# Initalise the Flask app
app = Flask(__name__)


# Loads pre-trained model



def cleaner(x):
    return [a for a in (''.join([a for a in x if a not in string.punctuation])).lower().split()]

Pipe = Pipeline([
    ('bow',CountVectorizer(analyzer=cleaner)),
    ('tfidf',TfidfTransformer()),
    ('classifier',DecisionTreeClassifier())
])

filename = './questionanswer.pkl'

model = pickle.load(open(filename, 'rb'))



cols = ['age', 'sex', 'bmi', 'children', 'smoker', 'region']

@app.route('/')
def home():
    return render_template("home.html")

@app.route('/predict',methods=['POST'])
def predict():
    int_features = [x for x in request.form.values()]
    #print('**********')
    #print(int_features)
    #print(int_features)
    #int_features = str(request.form.values())
    final = ' '.join([str(elem) for elem in int_features])
    #data_unseen =    final #pd.DataFrame([final], columns = cols)
    #prediction = model.predict(['What are you doing'])[0] # predict_model(model, data=data_unseen, round = 0)
    #print(len(final))
    #print('*******')
    #print([final])
    #print([final]))
    prediction = model.predict([final])[0]
    #prediction = int(prediction.Label[0])
    return render_template('home.html',pred='Kireka anasema: {}'.format(prediction))

@app.route('/predict_api',methods=['POST'])
def predict_api():
    data = request.get_json(force=True)
    #data_unseen = data #pd.DataFrame([data])
    prediction = model.predict(['What are you doing'])[0] # predict_model(model, data=data_unseen)
    output = prediction   #.Label[0]
    return jsonify(output)

if __name__ == '__main__':
    app.run(debug=True)




#print(model.predict(['What are you doing'])[0])