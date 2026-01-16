import numpy as np
import re
import nltk

#Pre-process the sentence and extract the words
def extract_words(sentence):
    ignore_words = ['a']
    words = nltk.word_tokenize(sentence)
    cleaned_words = [word.lower() for word in words if word not in ignore_words]
    return cleaned_words

#Build vocabulary dictionary
def build_vocabulary(sentences):
    word_list = []
    for sentence in sentences:
        words = extract_words(sentence)
        word_list.extend(words)
    
    word_list = sorted(list(set(word_list)))
    return word_list

def bagofwords(sentence, vector):
    sentence_words = extract_words(sentence)
    # frequency word count
    bag = np.zeros(len(vector))
    for sw in sentence_words:
        for i,word in enumerate(vector):
            if word == sw: 
                bag[i] += 1
                
    return np.array(bag)

file = open("corpora\dlmgr_corpora.txt", "r") 
corpora = file.readlines()
print(corpora)

vocabulary_vector = build_vocabulary(corpora)
#print(vocabulary_vector)

feature_vector = bagofwords('add me to DL-PP-TE',vocabulary_vector)
print(feature_vector)

 