"""
Sentiment analysis of text
"""
#Adding necessary imports
import csv
import re
import nltk
from nltk import pos_tag
from nltk import FreqDist
from nltk.corpus import stopwords
from sklearn import svm
from nltk.tokenize import word_tokenize,RegexpTokenizer
from nltk.stem import PorterStemmer,SnowballStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB,GaussianNB
from sklearn.linear_model import LogisticRegression as LR
from nltk.stem import WordNetLemmatizer
import pickle
import common as common
import numpy

#Arrays to store data after reading files
trainingData=[]
testData=[]

def read_data():
    #read training data to train model from csv file
    with open("DataSet/Training.csv","r") as file:
        reader=csv.reader(file)
        for row in reader:
            trainingData.append(row)

    #read test data to determine Accuracy and test model
    with open("DataSet/Test.csv","r") as file:
        reader=csv.reader(file)
        for row in reader:
            testData.append(row)

read_data()

#dictionary to enumerate emotion
emotionDictionary = {
    "angry" : 0,
    "happy" : 1,
    "sad" : 2,
    "relaxed" : 3
}

trainingSong=[[],[]]
testSong=[[],[]]

#Preparing datatset for model-training as 2D-array containing lyrics - emotion pair
def prepare_dataset():
    #5th element in each song record is emotion
    #6th element in each song record is lyrics
    for song in trainingData:
        trainingSong[1].append(emotionDictionary[song[4]])
        trainingSong[0].append(song[5])

    for song in testData:
        testSong[1].append(emotionDictionary[song[4]])
        testSong[0].append(song[5])

prepare_dataset()
print("Training over dataset (Emotion, lyrics):")
#Convert a collection of raw documents to a matrix of TF-IDF features.
vectorizer = TfidfVectorizer(tokenizer=common.tokenize_text, min_df=1, ngram_range = (1,4), sublinear_tf =True, stop_words = "english")

#apply tf-idf vectorizer on training and test data
train_x = vectorizer.fit_transform(trainingSong[0])
test_x = vectorizer.transform(testSong[0])

#Logistic Regression model
model = LR(multi_class='multinomial',solver='newton-cg')
#training model over training data
model.fit( train_x,trainingSong[1])
print("Accuracy by applying Logistic Regression: ")
print(model.score(test_x,testSong[1]))

# Saving the trained model using pickle
model_filename ='finalized_model.sav'
pickle.dump(model,open(model_filename,'wb'))

dictionary = 'dictionary.pkl'
pickle.dump(vectorizer,open(dictionary,'wb'))
print("Model saved as finalized_model.asv and vectorizer as dictionary.pkl")