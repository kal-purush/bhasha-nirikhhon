import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import LogisticRegression
import openai

df = pd.read_pickle('training_twitter_data.pkl')
encodes = pd.read_pickle'twitter_embeddings.pkl')

X = encodes
y = df['labels']
clf = LogisticRegression(random_state=0, class_weight='balanced').fit(X, y)

def encode(input_text):
  openai.api_key = None ## Replace this with your OpenAI API key
  response = openai.Embedding.create(
    input = input_text,
    model="text-similarity-davinci-001"
    )
  return response['data'][0]['embedding']

def classify_text(input_text):
  max_id = clf.predict_proba([encode(input_text)]).argmax()
  return clf.classes_[max_id]