import csv 
from sklearn.model_selection import train_test_split
import vsmlib
import torch
import re
import random
import numpy as np
import json
import gensim
from sklearn.linear_model import LogisticRegression
from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.util import *
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import textstat
from gensim.models.keyedvectors import KeyedVectors
from nltk.corpus import stopwords
import ast
from tqdm import tqdm
import copy
import ast
from numpy import arange,array,ones
from scipy import stats
import os

path_to_vsm = "word_linear_glove_500d"
vsm = vsmlib.model.load_from_dir(path_to_vsm)

def get_sequence_label(sequence):

    score_list = [tupl[4] for tupl in sequence]
    xi = arange(0, len(score_list))
    slope, intercept, r_value, p_value, std_err = stats.linregress(xi,score_list)
    line = slope*xi+intercept
    avg_part1 = sum(score_list[:3])/3
    avg_part2 = sum(score_list[3:])/len(score_list[3:])

    if (slope > .15 and max(score_list) > .75 or avg_part1 < .4 and avg_part2 > .6) and 0.0 not in score_list[:3] :
        return 1 #np.array([1])

    return 0 #np.array([0])

def getSeqEmbedding(sequence):
    #print(sequence)
    embeddings = []
    for seq in sequence:
        e = []
        tokens = seq[3].split(' ')
        for word in tokens:
            word = re.sub('[^A-Za-z0-9]+', '', word).lower()
            e.append(getEncoding(word))
            #print(len(e))
        embeddings.append(torch.from_numpy(np.array([e])).type(torch.FloatTensor))
    #print(embeddings[0].shape)
    return embeddings

def readData(filename):

    data = []
    labels = []
    ids = []

    with open(filename, 'r', encoding="utf-8") as csvfile:
        csvdata = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(csvdata)
        for line in csvdata:
            ids.append(line[0])
            ids.append(line[0])
            d = []
            d = line[1] + ' ' + line[2] + ' ' + line[3] + ' ' + line[4] + ' ' + line[5]
            #print(d)
            data.append(d)
            d = []
            d = line[1] + ' ' + line[2] + ' ' + line[3] + ' ' + line[4] + ' ' + line[6]
            #print(d)
            data.append(d)

            if filename != "test.csv":
                l = line[7]
                if l == '1':
                    labels.append(1)
                    labels.append(0)
                else:
                    labels.append(0)
                    labels.append(1)

    #get word embeddings
    embeddings = []
    for d in range(len(data)):
        tokens = data[d].split(' ')
        e = []
        for word in tokens:
            word = re.sub('[^A-Za-z0-9]+', '', word).lower()
            e.append(getEncoding(word))
        if filename != "test.csv":
            embeddings.append((torch.from_numpy(np.array([e])),labels[d]))
        else:
            embeddings.append((torch.from_numpy(np.array([e])),0))
    return ids, embeddings

def getTrainingAndValData(data, label):
    #X_train, X_test, Y_train, Y_test = readData()
    print(len(data))
    inputdata = []
    for d in data:
        temp = []
        for word in d:
            temp.append(word.squeeze(0).squeeze(0))
        inputdata.append(torch.stack(temp).unsqueeze(0))

    data_1 = []
    data_0 = []
    for i in range(len(inputdata)):
        if label[i]==1:
            data_1.append(inputdata[i])
        else:
            data_0.append(inputdata[i])
    
    # print(len(data_0))
    # print(len(data_1))
    random.shuffle(data_0)
    data_0 = data_0[:len(data_1)]

    data = []
    data = data_0
    data = data + data_1
    label = [1 for i in range(len(data_1)*2)]
    label[:len(data_1)] = [0] * len(data_1)

    l = []
    for i in range(len(data)):
        l.append(torch.from_numpy(np.array(int(label[i]))).type(torch.FloatTensor))

    X_train, X_test, Y_train, Y_test = train_test_split(data, l, test_size = 0.3, random_state=12345)
    print(len(X_test))
    return X_train, X_test, Y_train, Y_test

def getEncoding(word): 
    if(vsm.has_word(word)):
        return vsm.get_row(word)
    else:
        return np.zeros(500)  

def preprocessData(data, label, isTrain):
    d = []
    l = []

    if isTrain:
        data_1 = []
        data_0 = []
        for i in range(len(data)):
            if label[i]=='1':
                data_1.append(data[i])
            else:
                data_0.append(data[i])
        
        random.shuffle(data_0)
        data_0 = data_0[:len(data_1)]
    
        data = []
        data = data_0
        data = data + data_1
        label = [1 for i in range(len(data_1)*2)]
        label[:len(data_1)] = [0] * len(data_1)

    for i in range(len(data)):
        seq = []
        tokens = data[i][0].split(' ')

        for word in tokens:
            word = re.sub('[^A-Za-z0-9]+', '', word).lower()
            seq.append(getEncoding(word))
        d.append(torch.from_numpy(np.array([seq])).type(torch.FloatTensor))
        l.append(torch.from_numpy(np.array(int(label[i]))).type(torch.FloatTensor))

    return d, l

# train_ids, train_data = readData("train.csv")
# dev_ids, dev_data = readData("dev.csv")