import sys
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

train = pd.read_csv('../data/r8-train-all-terms.txt', header = None, sep = '\t')
test = pd.read_csv('../data/r8-test-all-terms.txt', header = None, sep = '\t')

train.columns = ['label', 'content']
test.columns = ['label', 'content']

# building a scikit-learn like class with fit, transform and fit_transform functions for ease
class GloVe_vectoriser:
    def __init__(self):
        print('Loading the pretrained word vectors..')
        word2vec = {}
        embedding = []
        idx2word = []
        with open('../data/glove.6B/glove.6B.50d.txt') as f:
            for line in f:
                values = line.split()
                word = values[0]
                vec = np.asarray(values[1:], dtype = 'float32')
                word2vec[word] = vec
                embedding.append(vec)
                idx2word.append(word)
        print('Found %s word vectors.' % len(word2vec))
        
        self.word2vec = word2vec
        self.embedding = np.array(embedding)
        self.word2idx = {v:k for k, v in enumerate(idx2word)}
        self.V, self.D = self.embedding.shape
    
    def fit(self, data):
        pass
    
    def transform(self, data):
        X = np.zeros((len(data), self.D))
        count = 0
        n = 0
        for sentence in data:
            tokens = sentence.lower().split()
            vecs = []
            for word in tokens:
                if word in self.word2vec:
                    vec = self.word2vec[word]
                    vecs.append(vec)
            if len(vecs) > 0:
                vecs = np.array(vecs)
                X[n] = vecs.mean(axis = 0)
            else:
                count = count + 1
            n = n + 1;
        print('No. of samples with no words found %s / %s' % (count, len(data)))
        return X
    
    def fit_transform(self, data):
        self.fit(data)
        return self.transform(data)
    
vectoriser = GloVe_vectoriser()

X_train = vectoriser.fit_transform(train.content)
y_train = train.label

X_test = vectoriser.transform(test.content)
y_test = test.label

from sklearn.ensemble import RandomForestClassifier
model = RandomForestClassifier(n_estimators = 200)
model.fit(X_train, y_train)

print('Train score:', model.score(X_train, y_train))
# Train score: 0.9992707383773929

print('Test score:', model.score(X_test, y_test))
# Test score: 0.9319323892188214, n_estimators = 100
# Test score: 0.9360438556418456, n_estimators = 200