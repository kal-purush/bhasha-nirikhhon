# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 16:13:50 2018

@author: Freek
"""
import pandas as pd
#import os
#print(os.getcwd())

with open('p022_names.txt', "r") as ins:
    for line in ins:
        items = line.replace('\"','').split(",")
        
items_sorted = sorted(items)
items_df = pd.DataFrame(items_sorted)
items_df.columns = ['Name']
items_df['Idx'] = items_df.index+1
#print(items_df[items_df.Idx==938])
#print(items_df.dtypes)

abc_list = {'A': 1,	'B': 2,	'C': 3,	'D': 4,	'E': 5,	'F': 6,	'G': 7,	'H': 8,	'I': 9,	'J': 10,	'K': 11,	'L': 12,	'M': 13,	'N': 14,	'O': 15,	'P': 16,	'Q': 17,	'R': 18,	'S': 19,	'T': 20,	'U': 21,	'V': 22,	'W': 23,	'X': 24,	'Y': 25,	'Z': 26}

score = []
for i in items_df['Name']:
    temp = []
    for letter in i:
        temp.append(abc_list[letter])
    score.append(sum(temp))

items_df['Score'] = score
#print(items_df[items_df.Idx==938])
items_df['total_score'] = items_df.Idx*items_df.Score
print('Total of all names scores: %d' %sum(items_df['total_score']))