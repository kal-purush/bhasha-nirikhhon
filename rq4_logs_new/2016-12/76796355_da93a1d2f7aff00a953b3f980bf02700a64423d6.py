import numpy as np
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer

zen = """Beautiful is better than ugly.
Explicit is better than implicit.
Simple is better than complex.
Complex is better than complicated.
Flat is better than nested.
Sparse is better than dense.
Readability counts.
Special cases aren't special enough to break the rules.
Although practicality beats purity.
Errors should never pass silently.
Unless explicitly silenced.
In the face of ambiguity, refuse the temptation to guess.
There should be one-- and preferably only one --obvious way to do it.
Although that way may not be obvious at first unless you're Dutch.
Now is better than never.
Although never is often better than *right* now.
If the implementation is hard to explain, it's a bad idea.
If the implementation is easy to explain, it may be a good idea.
Namespaces are one honking great idea -- let's do more of those!"""

docs = zen.split('\n')
#print(docs)

vectorizer = CountVectorizer(analyzer = 'word', ngram_range = (3, 3))
docs_vec = vectorizer.fit_transform(docs)
terms = np.sum(docs_vec.toarray(), axis = 0)
term_indices = np.argmax(terms)

for ind in np.nditer(term_indices):
  print '%s : %d' % (vectorizer.get_feature_names()[ind], terms[ind])

tfidf_vec = TfidfVectorizer(analyzer = 'word', ngram_range = (1, 1), norm = None)
docs_tf = tfidf_vec.fit_transform(docs)
terms_f = np.sum(docs_tf.toarray(), axis = 0)
terms_indices = np.argmax(terms_f)

for ind in np.nditer(term_indices):
  print '%s : %f' % (tfidf_vec.get_feature_names()[ind], terms_f[ind])