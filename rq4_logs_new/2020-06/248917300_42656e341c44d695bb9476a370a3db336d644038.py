import json
import pickle
import re
import string
import numpy as np
import multiprocessing as mp

from preprocess import preprocess_text

from nltk.corpus import stopwords
from nltk.corpus import words as engWords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.decomposition import NMF, LatentDirichletAllocation
from sklearn.metrics.pairwise import linear_kernel
from nltk.stem import WordNetLemmatizer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
no_features = 1000
analyzer = SentimentIntensityAnalyzer()
# translator = Translator()

def project(data, user, tfidf_model, nmf_model):
    texts = list(map(lambda x : x["text"] if x["user"]["id"] == user else " ", data))
    texts += list(map(lambda x : x["retweeted_status"]["text"] if("retweeted_status" in x.keys() and x["retweeted_status"]["user"]["id"] == user) else " ", data))
    texts += list(map(lambda x : x["quoted_status"]["text"] if("quoted_status" in x.keys() and x["quoted_status"]["user"]["id"] == user) else " ", data)) 
    
    ftexts = []
    for text in texts:
        if(text != " "):
            ftexts.append(text)
    # print(len(ftexts))
    ftexts = list(set(ftexts))
    # print(ftexts)
    with mp.Pool() as pool:
        result = pool.map(preprocess_text, ftexts)
    print(result)
    score_matrix = np.zeros((1, 3))
    sentiment_scores = np.zeros((1, 4))
    # result = ["rt sarthakgoswamii mohit dangana caa protest caa caanrcprotests jnuattack mohitdangana httpstco8wlgk0qwnu"]
    # result.append("say no to caa nrc npr fuck bjp fuck rss")
    for query in result:
        vectQuery = tfidf_model.transform([query])
        sentiments = analyzer.polarity_scores(query)
        temp = np.array([sentiments['pos'], sentiments['neg'], sentiments['neu'], sentiments['compound']])
        # Gives the probability of each topic for the query
        # in a matrix of (top_words * no_of_topics) form
        # i.e. which topic does each word belong to.
        # print(query)
        topic_probability_scores = nmf_model.transform(vectQuery)
        query_topic = np.sum(topic_probability_scores, axis=0)
        score_matrix = np.vstack((score_matrix, query_topic))
        sentiment_scores = np.vstack((sentiment_scores, temp))
    # print(score_matrix)
    user_score = np.sum(score_matrix, axis = 0)
    sentiment_score = np.mean(sentiment_scores, axis = 0)
    print("user topic projection score: ", user_score)
    print("user sentiment score: ", sentiment_score)

def display_topics(model, feature_names, no_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic %d:" % (topic_idx))
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-no_top_words - 1:-1]]))



if __name__ == "__main__":
    with open('../caa_raw_data.json', encoding = "utf-8", errors = "ignore") as f:
        data = json.load(f)
    with open("../data/lda_data.pk", 'rb') as fp:
        tf_model, tf_matrix, lda = pickle.load(fp)
    
    no_top_words = 20
    feature_names = tf_model.get_feature_names()
    display_topics(lda, feature_names, no_top_words)
    project(data, 111944435, tf_model, lda)
    