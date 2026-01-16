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
from sklearn.ensemble import RandomForestClassifier
import enchant


df = pd.read_csv("/Users/XS/Desktop/data_filtered.csv")
print(df)
training = np.array(df)
print(training.shape)


RF = RandomForestClassifier(n_estimators=100,criterion='entropy',max_features=80,max_depth=10,oob_score=True)
RF.fit(training[:,:6457],training[:,6457])
print(RF.feature_importances_)
print(RF.oob_score_)