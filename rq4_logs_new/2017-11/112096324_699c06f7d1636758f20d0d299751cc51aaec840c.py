# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import csv as csv
from sklearn.naive_bayes import MultinomialNB
import numpy as np
from pyvi.pyvi import ViTokenizer, ViPosTagger
import sys
import pickle

def prefix(arr):
    if arr[0] > 0.8:
        return '-1'
    elif arr[1] > 0.8:
        return '0'
    elif arr[2] > 0.9:
        return '1'
    else :
        return '0'

def read(filename):
    f = open(filename)
    data = pickle.load(f)
    f.close()
    return data


def makeMatrix(arr, dct):
    lst = []
    for x in dct:
        if x in arr:
            lst.append(1)
        else:
            lst.append(0)
    return lst


diction = read("diction.file")
lb = read("labels.file")
for x in range(len(lb)):
    if lb[x] == "":
        print(x)
lbl = set(lb)
print(lbl)
print(len(lb))

trd = read("trains.file")
count = 0
lpre = []
ldata = []
result = []
with open('data.csv') as csvDataFile:
    csvReader = csv.reader(csvDataFile)
    for row in csvReader:
        count = count + 1
        pre = unicode(row[0], "utf-8")
        ldata.append(row)
        lpre.append(pre)
# du doan
count = 0
rsd = []
for i in range(len(ldata)):
    lpre[i] = lpre[i].replace(",", "").replace(".", "")
    lpre[i] = ViTokenizer.tokenize(lpre[i])
    lpre[i] = lpre[i].lower()
    arrpre = lpre[i].split()
    apre = makeMatrix(arrpre, diction)
    dpre = np.array([apre])
    ## call MultinomialNB
    clf = MultinomialNB()
    # training
    clf.fit(trd, lb)
    # test
    # print(repr(pre).decode("unicode-escape"))
    # print('Predicting class of dpre:', str(clf.predict(dpre)[0]), clf.predict_proba(dpre), prefix(clf.predict_proba(dpre)[0]))
    print('Predicting class of dpre:', clf.predict_proba(dpre), prefix(clf.predict_proba(dpre)[0]))
    if str(prefix(clf.predict_proba(dpre)[0])) == '1':
        count = count + 1
        result.append(ldata[i])
    # ldata[i] = ldata[i] + [unicode(str(prefix(clf.predict_proba(dpre)[0])), "utf-8")]
    # result.append(ldata[i])
    # if str(clf.predict(dpre)[0]) != '1':
    #     result.append(ldata[i])

print(count)
with open('resultdata.csv', 'w') as csvoutput:
    writer = csv.writer(csvoutput)
    for j in range(len(result)):
        writer.writerow(result[j])
        # print(repr(lpre[0]).decode("unicode-escape"))