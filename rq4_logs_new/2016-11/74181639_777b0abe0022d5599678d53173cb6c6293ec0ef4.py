# -*- coding: utf-8 -*-

from sklearn.feature_extraction import DictVectorizer

import nltk
import pandas as pd
import re
import string
from string import digits
from collections import Counter
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import string
import numpy as np
import csv

# read data in as data frame
st = pd.read_csv('/Users/XS/Desktop/Project/HRC_train.tsv', sep = 'str("") | str("\t")',  names=['content'], engine='python')
df = pd.DataFrame(st.content.str.split('"\t"',1).tolist(), columns = ['label','content'])

# label
for i in df.index:
    df.label[i] = df.label[i][1]

'''
for j in [x for x in range(3505) if x not in [24,44,81,100,119,132,137,175,193,195,214,215,225,236,237,241]]:
    df.content[j] = re.search(r'subject: (.*?) (u.s. department of state case no.)', df.content[j]).group(1)
'''


# get rid of punctuations and numbers
translator = str.maketrans({key: None for key in string.punctuation})
translator_2 = {ord(k): None for k in digits}
df['upd_content'] = df.content
df['content_non_punc'] = df.content

for i in df.index:
	df['upd_content'][i] = df.content[i].translate(translator)
	df['content_non_punc'][i] = df['upd_content'][i].translate(translator_2)

# tokenize all content
df['token'] = df['content_non_punc'].apply(nltk.word_tokenize)

# steming correction
df['new_token'] = df.token
stop_words = set(stopwords.words('english'))
stop_words.update(['fw', 'subject', 'case', 'doc', 'unclassified', 'a','b','c','d','e','f','g','h''i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'])
porter = PorterStemmer()
for i in df.index:
	email = df.token[i] 
	list_of_words = [porter.stem(j.lower()) for j in email if j.lower() not in stop_words]
	df.new_token[i] = list_of_words

# create a dictionary in the form {word1:count1,...}
dictionary = df.new_token.apply(Counter)
# print(dictionary)

# convert the dictionary into a data frame
ve = DictVectorizer(sparse = False)
matrix = ve.fit_transform(dictionary)
names = ve.get_feature_names()
df = pd.DataFrame(matrix,columns=names)
print(df)
# str1 = ','.join(names)
# np.savetxt("/Users/XS/desktop/data.csv",matrix,delimiter=',',header = str1)

df.to_csv('/Users/XS/desktop/df.csv', index=False, header=True, sep=',')




# analyze data frame to find power feature