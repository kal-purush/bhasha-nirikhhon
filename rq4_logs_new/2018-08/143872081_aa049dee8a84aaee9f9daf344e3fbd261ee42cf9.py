# -*- coding: utf-8 -*-
"""
Created on Mon Aug 27 17:57:30 2018

@author: Freek
"""

#import os
import pandas as pd
path = 'p42_words.txt'
triangle_numbers = []

for i in range(1,30):
    triangle_numbers.append(int(0.5*i*(i+1)))
    
with open(path, "r") as ins:
    for line in ins:
        items = line.replace('\"','').split(",")

items_df = pd.DataFrame(items)
items_df.columns = ['Word']

abc_list = {'A': 1, 'B': 2, 'C': 3, 'D': 4,	'E': 5,	'F': 6,	'G': 7,	'H': 8,	'I': 9,	'J': 10,	'K': 11,	'L': 12,	'M': 13,	'N': 14,	'O': 15,	'P': 16,	'Q': 17,	'R': 18,	'S': 19,	'T': 20,	'U': 21,	'V': 22,	'W': 23,	'X': 24,	'Y': 25,	'Z': 26}

score = []
for i in items_df['Word']:
    temp = []
    for letter in i:
        temp.append(abc_list[letter])
    if sum(temp) in triangle_numbers:
        score.append(i)

print('%d words are triangle' %len(score))