import pandas as pd
import jieba
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.cluster import KMeans



def del_stop_words(words,stopwords):

    new_words = []
    for w in words:
        if w not in stopwords:
            new_words.append(w)
    return new_words

def preprocess_data(corpus_path, stopwords):
    corpus = []
    with open(corpus_path, 'r', encoding='utf-8') as f:
        for line in f:
            corpus.append(' '.join([word for word in jieba.lcut(line.strip()) if word not in stopwords]))
    return corpus

def get_text_tfidf_matrix(corpus):
    transformer = TfidfTransformer()
    vectorizer = CountVectorizer()
    
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))

    weights = tfidf.toarray()
    return weights



f = open('stopword.txt','r')
ls = f.readlines()
stop = []
for l in ls:
    stop.append(l[:-1])
stop.append('\n')

f = open('comments.txt','r')
ls = f.readlines()
df = pd.DataFrame(ls)

df['cut'] = df[0].apply(jieba.lcut)

df['stop'] = df['cut'].apply(del_stop_words, args=[stop])

corpus = preprocess_data('test.txt', stop)
weights = get_text_tfidf_matrix(corpus)


