# -*- coding: utf-8 -*-
"""
Created on Sat Jun  6 21:01:48 2020

@author: Soumitra
"""

"""
SECTION 1: 
Here I made an API call using my personal API key.
The Merriam Webster Dictionary
returned the json object which
was converted to dictionary
in python.
"""

import requests
word = input("Enter a word: ")
url = 'https://dictionaryapi.com/api/v3/references/collegiate/json/'+word+'?key=acce992a-5cd9-40e4-8f23-4022f7547159'
r = requests.get(url)
data = r.json()

#%%
"""
Section 2:
The defination of the word was extracted from the dictionary.
"""
for k in range(3):
    try:
        if(data[k]):
            print(data[k]["fl"],":")
            for i in range(len(data[k]["shortdef"])): 
                print(i,'-',data[k]["shortdef"][i])
    except IndexError:
        continue
    
      