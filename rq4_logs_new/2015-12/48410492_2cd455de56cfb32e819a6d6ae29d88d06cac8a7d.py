# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 00:45:02 2015

@author: Ashish Yadav
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 00:06:57 2015

@author: Ashish Yadav
"""

import pandas as pd
from sklearn import svm
from sklearn.naive_bayes import GaussianNB
from sklearn import datasets
import numpy as np
from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier


def measure(word1,word2):
    if word1==word2:
        return 1
    else:
        return 0

def main():
    df = pd.read_csv('Data_Formatted2.csv')
    Y = df.values

    num_of_rows = Y.shape[0]
    ans = ""
    for i in range(num_of_rows):
        if Y[i,3] != 1:
            ans = ans + str(Y[i,3])
            
    word_list = ans.split()
    unique_words = set(word_list)
    vocabulary = list(unique_words)
    
    questions_list = []
    answers_list = []
    answers = []
    answers2 = []
    label = []
    for i in range(num_of_rows):
        if Y[i,1] == 1:
            answers_list.append(answers2)
            questions_list.append(Y[i,3])
            answers2 = []
        else:
            answers2.append(Y[i,3])
            if Y[i,6]>0:
                label.append(1)
            else:
                label.append(-1)
    
    answers_list.append(answers2)
    del answers_list[0]
    feature_vector = []
    print "ans"
    
    #print answers_list
    #print len(answers_list)
    #print len(label)
    count_ans = 0
    #print answers_list
    for i in range(len(answers_list)):
        questions_list_split = questions_list[i].split()
        lis = answers_list[i]
        for j in range(len(lis)):            
            val = lis[j]
            if (isinstance(val,basestring)):
                val_list = val.split()                
            else:
                del label[count_ans]
                continue                
            count_ans = count_ans + 1                
                
            vocab_zeros = [0] * len(vocabulary)
            for k in range(len(val_list)):
                sum_simil = 0;
                for l in range(len(questions_list_split)):
                    sum_simil = sum_simil + measure(val_list[k],questions_list_split[l])
                avg_simil = float(sum_simil)/len(questions_list_split)
                if val_list[k] in vocabulary:
                    vocab_zeros[vocabulary.index(val_list[k])] = avg_simil
            feature_vector.append(vocab_zeros)
    #print len(feature_vector)
    #print np.asarray(feature_vector)
    #print np.asarray(label)
    
    boost = AdaBoostClassifier(DecisionTreeClassifier())
    y_pred = boost.fit(feature_vector, label).predict(feature_vector)   
    print y_pred

    
if __name__=="__main__":
    main()