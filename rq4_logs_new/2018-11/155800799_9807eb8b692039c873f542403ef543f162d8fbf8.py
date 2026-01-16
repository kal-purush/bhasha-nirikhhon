import csv
import pickle
import numpy as np
import pandas as pd
import math
from nltk import ngrams
import random
import os
import sys
from collections import Counter
import time


def get_ngrams(x, n):
    list_grams = []

    print(len(x))
    for i in range(len(x)):
        if (i%10000==0):
            print(i)

        counter = Counter(ngrams(x[i].split(" "),n))
        counter['affine_expansion_constant_ml4771'] = 1
        list_grams.append(counter)
    return list_grams


def count_idf(x):
    d = len(x)
    idf = {}
    for i in range(d):
        for each in x[i]:
            idf[each] = idf.get(each, 0) + 1
    for key, value in idf.items():
        idf[key] = math.log10(d/value)
    return idf


def online_perceptron(list_counter_in, y, idf=None, tfidf=False):
    if tfidf:
        list_counter=list([])
        for i in range(len(list_counter_in)):
            dict = {}
            for key in list_counter_in[i]:
                dict[key] = list_counter_in[i][key]*idf[key]
            list_counter.append(dict)
    else:
        list_counter=list_counter_in

    n = len(y)
    seq = list(range(n))

    unique = set([])
    w_t = {}
    print('first cycle')
    random.shuffle(seq)
    c=0
    for i in seq:
        c+=1
        if c % 10000 == 0:
            print("i:{}, dimensions:{}".format(c, len(w_t)))

        dotprod = sum(list_counter[i][gram]*w_t.get(gram, 0) for gram in list_counter[i])
        if y[i]*dotprod <= 0:
            for gram, value in list_counter[i].items():
                w_t[gram] = w_t.get(gram, 0) + y[i] * value
                unique.add(gram)


    w_avg = {}
    print('dimensions: {}'.format(len(w_t.items())))
    print('unique: {}'.format(len(unique)))

    for gram, value in w_t.items():
        w_avg[gram] = w_avg.get(gram, 0) + value

    random.shuffle(seq)
    print('second cycle')
    changes = {}
    changes_i = {}
    c = 0
    for i in seq:
        c += 1
        if c%10000==0:
            print("{} out of {}".format(c, n))


        #start_time = time.time()
        dotprod = sum(list_counter[i][gram]*w_t.get(gram, 0) for gram in list_counter[i])
        #end_time = time.time()
        #print('time to calculate dotproduct: {}'.format(end_time-start_time))


        if y[i]*dotprod<=0:
            #start_time = time.time()
            for gram, value in list_counter[i].items():
                w_t[gram] = w_t.get(gram, 0) + y[i] * value
                if gram not in changes:
                    changes[gram] = []
                changes[gram].append(y[i]*value)
                if gram not in changes_i:
                    changes_i[gram] = []
                changes_i[gram].append(c)
            #end_time = time.time()
            #print('time to update weights: {}'.format(end_time-start_time))

    print('calculating average w:')
    start_time = time.time()
    for gram, changei in changes_i.items():
        # w_avg[gram] = w_avg[gram] / (n + 1)
        if gram not in w_avg:
            w_avg[gram] = 0
        x = w_avg[gram]
        w_avg[gram] = w_avg[gram] * changei[0]
        #print('{}: {}'.format(gram, w_avg[gram]))
        for i in range(len(changei) - 1):
            #print('{}: {}'.format(gram, w_avg[gram]))
            w_avg[gram] += (changei[i + 1] - changei[i]) * (changes[gram][i] + x)
            x += changes[gram][i]
            if i + 1 == len(changei) - 1:
                #print('{}: {}'.format(gram, w_avg[gram]))
                w_avg[gram] += (n - changei[i + 1]) * (changes[gram][i + 1] + x)
    end_time = time.time()
    print(end_time-start_time)

    return w_avg


def get_error(tf, labels, w, idf=None, tfidf=False):
    n = len(labels)
    error = 0
    for i in range(n):
        dotprod = 0
        for gram, value in tf[i].items():
            if tfidf and gram not in idf:
                continue

            if gram not in w:
                w_i = 0
            else:
                w_i = w[gram]

            if tfidf:
                dotprod += value*idf[gram]*w_i
            else:
                dotprod += value * w_i
        if labels[i] * dotprod <=0:
            error += 1
    print("total error is: {}".format(error))
    return error/n


def get_2_misclassified(test_x, tf, labels, w):
    n = len(labels)
    error = 0
    while error < 2:
        i = random.randint(0,n)
        dotprod = sum(tf[i][gram] * w.get(gram, 0) for gram in tf[i])
        if labels[i] * dotprod <= 0:
            error += 1
            print('misclassified text: {}'.format(test_x[i]))
            print(tf[i])
    return


if __name__ == '__main__':

    print('loading original training data...')

    df = pd.read_csv('reviews_tr.csv', sep=',')
    training = np.array(df.values)

    print('loading original test data...')

    df = pd.read_csv('reviews_te.csv', sep=',')
    test = np.array(df.values)

    if training[0,1]=='text':
        train_x = training[1:, 1]
        test_x = test[1:, 1]
    else:
        train_x = training[:,1]
        test_x = test[:, 1]

    if training[0,0] == 'rating':
        train_y = training[1:, 0]
        test_y = test[1:, 0]
    else:
        train_y = training[:,0]
        test_y = test[:, 0]

    train_y[train_y == 0] = -1
    test_y[test_y == 0] = -1

    # with open('train_x.pickle', 'wb') as file:
    #     pickle.dump(train_x, file, protocol=pickle.HIGHEST_PROTOCOL)
    # with open('train_y.pickle', 'wb') as file:
    #     pickle.dump(train_y, file, protocol=pickle.HIGHEST_PROTOCOL)
    # with open('test_x.pickle', 'wb') as file:
    #     pickle.dump(test_x, file, protocol=pickle.HIGHEST_PROTOCOL)
    # with open('test_y.pickle', 'wb') as file:
    #     pickle.dump(test_y, file, protocol=pickle.HIGHEST_PROTOCOL)
    #
    # print('finish saving pickle files')
    #

    # print('loading files..')
    # with open('train_x.pickle', 'rb') as file:
    #     train_x = pickle.load(file)
    # with open('train_y.pickle', 'rb') as file:
    #     train_y = pickle.load(file)
    # with open('test_x.pickle', 'rb') as file:
    #     test_x = pickle.load(file)
    # with open('test_y.pickle', 'rb') as file:
    #     test_y = pickle.load(file)

    print('get training unigrams')
    tr_unigram_repr = get_ngrams(train_x, 1)

    print('get test unigrams')
    te_unigram_repr = get_ngrams(test_x, 1)

    # print('saving training_unigram')
    # with open('tr_unigram_repr.pickle', 'wb') as file:
    #     pickle.dump(tr_unigram_repr, file, protocol=pickle.HIGHEST_PROTOCOL)
    # print('saving test')
    # with open('te_unigram_repr.pickle', 'wb') as file:
    #     pickle.dump(te_unigram_repr, file, protocol=pickle.HIGHEST_PROTOCOL)
    #
    # print('loading training unigrams..')
    # start_time = time.time()
    # with open('tr_unigram_repr.pickle', 'rb') as file:
    #     tr_unigram_repr = pickle.load(file)
    # end_time = time.time()
    # print(end_time-start_time)


    print('training online perceptron')
    w_1 = online_perceptron(tr_unigram_repr, train_y)

    w1_sorted = sorted(w_1.items(), key=lambda kv: kv[1])
    lowest_weights = w1_sorted[:10]
    highest_weights = w1_sorted[-10:]

    print('lowest words:')
    for each in lowest_weights:
        print(each)
    print('highest words:')
    for each in highest_weights:
        print(each)



    print('calculating training error - unigrams')
    unigram_training_error = get_error(tr_unigram_repr, train_y, w_1)
    print(unigram_training_error)


    print('loading test unigrams..')
    start_time = time.time()
    with open('te_unigram_repr.pickle', 'rb') as file:
        te_unigram_repr = pickle.load(file)
    end_time = time.time()
    print(end_time-start_time)

    print('get test unigrams')
    te_unigram_repr = get_ngrams(test_x, 1)

    print('calculating test error - unigrams')
    unigram_test_error = get_error(te_unigram_repr, test_y, w_1)
    print(unigram_test_error)

    get_2_misclassified(test_x, te_unigram_repr, test_y, w_1)

    '''
    IDF
    '''
    print('computing tfidf')
    idf = count_idf(tr_unigram_repr)
    print('training online perceptron - tfidf')
    w_tfidf = online_perceptron(tr_unigram_repr, train_y, idf, tfidf=True)

    print('calculating training error - tfidf')
    tfidf_training_error = get_error(tr_unigram_repr, train_y, w_tfidf, idf, tfidf=True)
    print(tfidf_training_error)

    print('computing test-tfidf')
    idf_test = count_idf(te_unigram_repr)
    print('calculating test error = tfidf')
    tfidf_test_error = get_error(te_unigram_repr, test_y, w_tfidf, idf_test, tfidf=True)
    print(tfidf_test_error)

    """
    BIGRAMS
    """
    print('get training bigrams')
    tr_bigram_repr = get_ngrams(train_x, 2)
    print('adding tr_unigram to bigrams')
    for i in range(len(tr_bigram_repr)):
        if i%10000==0:
            print(i)
        tr_bigram_repr[i] = tr_unigram_repr[i] + tr_bigram_repr[i]

    print('get test bigrams')
    te_bigram_repr = get_ngrams(test_x, 2)
    print('adding te_unigrams to bigrams')
    for i in range(len(te_bigram_repr)):
        if i%10000==0:
            print(i)
        te_bigram_repr[i] = te_unigram_repr[i] + te_bigram_repr[i]


    print('training online perceptron - bigrams')
    w_2 = online_perceptron(tr_bigram_repr, train_y)

    print('calculating training error - bigrams')
    bigram_training_error = get_error(tr_bigram_repr, train_y, w_2)
    print(bigram_training_error)


    print('calculating test error - bigrams')
    bigram_test_error = get_error(te_bigram_repr, test_y, w_2)
    print(bigram_test_error)

    '''
    TRIGRAMS
    '''

    print('get training trigrams')
    tr_trigram_repr = get_ngrams(train_x, 3)
    print('adding tr_bigrams to trigrams')
    for i in range(len(tr_trigram_repr)):
        if i%10000==0:
            print(i)
        tr_trigram_repr[i] = tr_bigram_repr[i] + tr_trigram_repr[i]

    print('get test trigrams')
    te_trigram_repr = get_ngrams(test_x, 3)
    print('adding te_bigrams to trigrams')
    for i in range(len(te_trigram_repr)):
        if i%10000==0:
            print(i)
        te_trigram_repr[i] = te_bigram_repr[i] + te_trigram_repr[i]



    print('training online perceptron - trigrams')
    w_3 = online_perceptron(tr_trigram_repr, train_y)

    print('calculating training error - trigrams')
    trigram_training_error = get_error(tr_trigram_repr, train_y, w_3)
    print(trigram_training_error)



    print('calculating test error - trigrams')
    trigram_test_error = get_error(te_trigram_repr, test_y, w_3)
    print(trigram_test_error)
