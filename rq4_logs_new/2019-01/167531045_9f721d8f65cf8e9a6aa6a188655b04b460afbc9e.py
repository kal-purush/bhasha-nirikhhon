
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import  PorterStemmer
import matplotlib.pyplot as mlt
import math
import pandas as pd 
import numpy as np
import seaborn as sns
from sklearn.feature_extraction.text import CountVectorizer
import warnings
import json
from sklearn.model_selection import  train_test_split


sms_table=pd.read_csv('spam.csv',encoding='latin-1')

sms_table.head()

sms_table.shape

sms_table[sms_table['v1']=='ham'].shape

sms_table[sms_table['v1']=='spam'].shape

##there are more ham messages than spam
new_sms_table=sms_table.iloc[:,:2]

new_sms_table.shape

new_sms_table.rename(columns={"v1": "labels", "v2": "sms"},inplace=True)

new_sms_table.head()

#new_sms_table['size']=new_sms_table['sms'].apply(len)

#new_sms_table.head()

dummies={'spam':1,'ham':0}

new_sms_table['labels']=new_sms_table['labels'].replace(dummies)

new_sms_table.labels = pd.to_numeric(new_sms_table.labels, errors='coerce')

new_sms_table.head()

new_sms_table['labels'].value_counts()

X=new_sms_table['sms']
Y=new_sms_table['labels']
train_X,test_X,train_Y,test_Y=train_test_split(X,Y,test_size=0.2, random_state=42)

train_X.head()

train_Y.head()

test_X.head()

test_Y.head()

#preprocessing of messages
def proc(sms, lower_case = True, stem = True, stop_words = True, gram = 2):
    sms= sms.lower()
    words = word_tokenize(sms)
    stopword = stopwords.words('english')
    words = [word for word in words if word not in stopword]
    stemmer = PorterStemmer()
    words = [stemmer.stem(word) for word in words]  
    words = [w for w in words if len(w) > 2]
    if gram > 1:
        w = []
        for i in range(len(words) - gram + 1):
            w += [' '.join(words[i:i + gram])]
        return w
    return words


ax = sns.countplot(x="labels", data=new_sms_table)



new_sms_table.describe()

new_sms_table.groupby('labels').describe()

new_sms_table['sms'].head(5).apply(processing)

cbow = CountVectorizer(analyzer=processing).fit(new_sms_table['sms'])

print(len(cbow.vocabulary_))

from sklearn.pipeline import Pipeline
import pickle
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.metrics import  classification_report,confusion_matrix
from sklearn.naive_bayes import MultinomialNB
pipeline = Pipeline([
   ( 'bow',CountVectorizer(analyzer=processing)),
    ('tfidf',TfidfTransformer()),
    ('classifier',MultinomialNB()),
])

model=pipeline.fit(train_X,train_Y)

predictions = model.predict(test_X)

print(confusion_matrix(test_Y,predictions))

print(classification_report(predictions,test_Y))

modelfname = 'spam_ham.pkl'
pickle.dump(model ,open(modelfname, 'wb'))

loaded_model = pickle.load(open(modelfname, 'rb'))
result = loaded_model.score(test_X,test_Y)
print(result)

model.predict(['Free entry in 2 a wkly comp to win FA Cup finally '])

loaded_model.predict(['Free entry in 2 a wkly comp to win FA Cup finally '])

model.predict(["Hi how was your day ?"])

loaded_model.predict(['how was your day'])

#model completed