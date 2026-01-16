#!/usr/bin/python
# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd

data = pd.read_csv('data/movielens/user_ratedmovies.dat', delimiter='\t')
item_info = pd.read_csv('data/movielens/movies.dat', delimiter='\t')
itemid2name = dict(zip(item_info['id'].tolist(), item_info['title'].tolist()))

N = len(set(data['userID'].tolist()))  # number of user
M = len(set(data['movieID'].tolist()))  # number of movie

rating_matrix = np.zeros([N, M])
userid2index = {}
itemid2index = {}
userid2itemindexes = {}

for i, row in data.iterrows():
    userid = row['userID']
    itemid = row['movieID']
    rating = row['rating']
    # print userid, itemid, rating
    if userid in userid2index:
        userindex = userid2index[userid]
        userid2itemindexes[userid].append(itemid)
    else:
        userindex = len(userid2index)
        userid2index[userid] = userindex
        userid2itemindexes[userid] = [itemid]

    if itemid in itemid2index:
        itemindex = itemid2index[itemid]
    else:
        itemindex = len(itemid2index)
        itemid2index[itemid] = itemindex

    rating_matrix[userindex, itemindex] = rating

index2userid = {y: x for x, y in userid2index.items()}
index2itemid = {y: x for x, y in itemid2index.items()}

nonzero_col, nonzero_row = rating_matrix.nonzero()
inds = zip(nonzero_col.tolist(), nonzero_row.tolist())

import sys


sys.path.append('../tpmrec/')

from mf import MF

mf = MF(rating_matrix, inds, 10, 0.0001, 0.01)


mf.train(10)

for userindex in range(1000):
    userid = index2userid[userindex]
    if len(userid2itemindexes[userid]) > 20:
        continue
    pr = mf.predict()
    user_predict = pr[userindex, :]
    top_item_indexes = np.argsort(user_predict)[::-1][:10]
    print "userid = ", userid
    for itemid in userid2itemindexes[userid]:
        print itemid, itemid2name[itemid]
    print "recommend item"
    for itemindex in top_item_indexes:
        itemid = index2itemid[itemindex]
        print itemid, itemid2name[itemid]



def read_data():
    f = open('data/movielens/user_ratedmovies.dat', 'r')
    line = f.readline().stirp('\n')
    while line:


    f.close