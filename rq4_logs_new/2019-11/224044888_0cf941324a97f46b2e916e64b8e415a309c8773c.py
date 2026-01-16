import csv 
from sklearn.model_selection import train_test_split
import vsmlib
import torch
import re
import random
import numpy as np
from textblob import TextBlob
from vaderSentiment import vaderSentiment
from sklearn.linear_model import LogisticRegression

path_to_vsm = "word_linear_glove_500d"
vsm = vsmlib.model.load_from_dir(path_to_vsm)

def readData(filename):
    data = []
    input = []
    output = []
    ids = []
    #sentiment  = []
    with open(filename, 'r', encoding="utf-8") as csvfile:
        csvdata = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(csvdata)
        analyzer = vaderSentiment.SentimentIntensityAnalyzer()
        for line in csvdata:
            d = []
            i = []
            ids.append(line[0])
            i.append(line[1])
            sentiment = analyzer.polarity_scores(line[1])
            d.append(sentiment['neg'])
            d.append(sentiment['neu'])
            d.append(sentiment['pos'])
            #d.append(TextBlob(line[1]).sentiment)

            i.append(line[2])
            sentiment = analyzer.polarity_scores(line[2])
            d.append(sentiment['neg'])
            d.append(sentiment['neu'])
            d.append(sentiment['pos'])
            #d.append(TextBlob(line[2]).sentiment)
            
            i.append(line[3])
            sentiment = analyzer.polarity_scores(line[3])
            d.append(sentiment['neg'])
            d.append(sentiment['neu'])
            d.append(sentiment['pos'])
            #d.append(TextBlob(line[3]).sentiment)

            i.append(line[4])
            sentiment = analyzer.polarity_scores(line[4])
            d.append(sentiment['neg'])
            d.append(sentiment['neu'])
            d.append(sentiment['pos'])
            #d.append(TextBlob(line[4]).sentiment)

            i.append(line[5])
            sentiment = analyzer.polarity_scores(line[5])
            d.append(sentiment['neg'])
            d.append(sentiment['neu'])
            d.append(sentiment['pos'])
            #d.append(TextBlob(line[5]).sentiment)

            i.append(line[6])
            sentiment = analyzer.polarity_scores(line[6])
            d.append(sentiment['neg'])
            d.append(sentiment['neu'])
            d.append(sentiment['pos'])
            #d.append(TextBlob(line[6]).sentiment)

            input.append(i)
            data.append(d)
            e1 = line[5].split(' ')
            d.append(len(e1))
            e2 = line[6].split(' ')
            d.append(len(e2))
            if(filename != "test.csv"):
                output.append(line[7])

    print(i[5])
    print(d[5])
    return data, output, ids

def getTrainingAndValData(data, output, size):
    X_train, X_test, Y_train, Y_test = train_test_split(data, output, test_size = size, random_state=1234)
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

def main():
    train_data, train_labels, train_ids = readData("train.csv")
    dev_data, dev_labels, dev_ids = readData("dev.csv")
    test_data, test_labels, test_ids = readData("test.csv")

    clf = LogisticRegression(solver='lbfgs').fit(train_data, train_labels)
    pred_train = clf.predict(train_data)
    count = 0
    for i in range(len(pred_train)):
        if(pred_train[i]!=train_labels[i]):
            print(i)
            count+=1
    print(count/len(pred_train))

    count  = 0
    pred = clf.predict(dev_data)
    pred_test = clf.predict(test_data)
    with open("output.csv",'w') as f:
        f.write("Id,Prediction\n")
        for i in range(len(pred_test)):
            f.write(test_ids[i])
            f.write(',%s\n' % pred_test[i])
            count+=1
    print(pred_test)
    prob = clf.predict_proba(dev_data)
    #print(prob)

    score = clf.score(dev_data, dev_labels)
    print(score)

main()

#readData("train.csv")