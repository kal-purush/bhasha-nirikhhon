import pandas as pd
from gensim.models.keyedvectors import KeyedVectors
from sklearn.svm import LinearSVC
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.metrics import accuracy_score

W2V_FILE = "/Users/chenyujing/Desktop/wiki.zh.vec"


def to_vector(row,i):
    words = row.split()
    vec = np.zeros([300])
    if i == 0:
        if words[-1] in model.wv:
            vec += model.wv[words[-1]]
    elif i == 1:
        for word in words:
            if words != '<CL>':
                if word in model.wv:
                    vec += model.wv[word]
    elif i == 2:
        for word in words[:-1]:
            if words != '<CL>':
                if word in model.wv:
                    vec += model.wv[word]

    return vec

if __name__ == '__main__':
    df = pd.read_csv('new.txt',encoding='utf-8',sep='\t',names=['features','label'])
    # df = df[df['label'].map(len) < 2]
    X_train, X_test, Y_train, Y_test = train_test_split(df['features'], df['label'], test_size=.2, random_state=1)
    model = KeyedVectors.load_word2vec_format(W2V_FILE, binary=False)

    # i = 0 for feature set 1
    # i = 1 for feature set 2
    # i = 2 for feature set 3
    for i in [0,1,2]:
        vector_X_train = [to_vector(x,i) for x in X_train]
        vector_X_test =[to_vector(x,i) for x in X_test]

        Y_train_lable = Y_train.tolist()
        Y_test_lable = Y_test.tolist()

        svc_model = LinearSVC()
        svc_model.fit(vector_X_train, Y_train_lable)
        predicted = svc_model.predict(vector_X_test)
        print('======================================')
        print(accuracy_score(Y_test_lable, predicted))
        print(metrics.classification_report(Y_test_lable, predicted))
        print('======================================')
