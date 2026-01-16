import nltk
import numpy as np
# nltk.download('punkt')
from nltk.stem.porter import PorterStemmer
stemmer = PorterStemmer()

def tokenize(sentance):
    return nltk.word_tokenize(sentance)

def stem(word):
    return stemmer.stem(word.lower())

def bag_of_words(tokenized_sentance, allwords):
    tokenized_sentance = [stem(w) for w in tokenized_sentance]
    bag = np.zeros(len(allwords), dtype=np.float32)

    for idx, w in enumerate(allwords): #enumarate give a index of each iterable in a list
        if w in tokenized_sentance:
            bag[idx] = 1.0

    return bag

# sentance = ["hello", 'how', 'are', 'you']
# words = ["hi", "hello", "I", "you", "bye"]
# print(bag_of_words(sentance, words)) 