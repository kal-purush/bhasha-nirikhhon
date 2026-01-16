import logging

import flask
from flasgger import Swagger
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import pandas as pd
import string
import json
import numpy as np
import re
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
import pickle
from sklearn import metrics
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score, confusion_matrix, precision_score, recall_score, f1_score, classification_report

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

#Load models from file

with open('Artifacts/logit_pit_2gram_1.pkl', 'rb') as filehandle:
    logit_pit_2gram_1 = pickle.load(filehandle) 
    
#Load tf-idf vectorizors from file

with open('Artifacts/pit_tfidf2.pkl', 'rb') as filehandle:
    pit_tfidf2 = pickle.load(filehandle)  
    


# NOTE this import needs to happen after the logger is configured


# Initialize the Flask application
application = Flask(__name__)

application.config['ALLOWED_EXTENSIONS'] = set(['pdf'])
application.config['CONTENT_TYPES'] = {"pdf": "application/pdf"}
application.config["Access-Control-Allow-Origin"] = "*"


CORS(application)

swagger = Swagger(application)

def clienterror(error):
    resp = jsonify(error)
    resp.status_code = 400
    return resp


def notfound(error):
    resp = jsonify(error)
    resp.status_code = 404
    return resp


@application.route('/v1/performance', methods=['POST'])
def sentiment_classification():
    """Run sentiment classification given text.
        ---
        parameters:
          - name: body
            in: body
            schema:
              id: text
              required:
                - text
              properties:
                text:
                  type: string
            description: the required text for POST method
            required: true
        definitions:
          SentimentResponse:
          Project:
            properties:
              status:
                type: string
              ml-result:
                type: object
        responses:
          40x:
            description: Client error
          200:
            description: Sentiment Classification Response
            examples:
                          [
{
  "status": "success",
  "sentiment": "1"
},
{
  "status": "error",
  "message": "Exception caught"
},
]
        """
    json_request = request.get_json()
    if not json_request:
        return Response("No json provided.", status=400)
    text = json_request['text']
    
    pitchers = [text]
    #To lower case
    pitchers = [x.lower() for x in pitchers]
    #Remove punctuation
    pitchers = [''.join(c for c in s if c not in string.punctuation) for s in pitchers]
    #Remove numbers
    pattern = '[0-9]'
    pitchers = [re.sub(pattern, '', i) for i in pitchers]
    
    pitchers_df = pit_tfidf2.transform(pitchers).toarray()
    pitchers_df = pd.DataFrame(pitchers_df)
    
    
    if text is None:
        return Response("No text provided.", status=400)
    else:
        label = list(logit_pit_2gram_1.predict(pitchers_df))
        return flask.jsonify({"status": "success", "label": label})


@application.route('/v1/sentiment/categories', methods=['GET'])
def sentiment_categories():
    """Possible sentiment categories.
        ---
        definitions:
          CategoriestResponse:
          Project:
            properties:
              categories:
                type: object
        responses:
          40x:
            description: Client error
          200:
            description: Sentiment Classification Response
            examples:
                          [
{
  "categories": [1,2,3],
  "sentiment": "1"
}
]
        """
    return flask.jsonify({"categories": list(range(1,6))})


if __name__ == '__main__':
    application.run(debug=True, use_reloader=True)