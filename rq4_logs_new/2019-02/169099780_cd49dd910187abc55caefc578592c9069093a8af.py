#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 12:00:46 2019

@author: Qingzhi Hu
"""

import re, string, timeit
from autocorrect import spell
l= []
f = open("/Users/hu/Downloads/queueingT/tex_files/usefulidentities.tex").read()
fnew = re.sub('[^a-zA-Z0-9\n\.]', ' ', f)
exclude = set(string.punctuation)

words = []
for word in fnew.split():
    s = ''.join(ch for ch in word if ch not in exclude)
    words.append(s)
    

for word in words:
    if len(word) > 3 and spell(word) != word: 
        if (word not in l):
            l.append(word)
            print(word, spell(word))
