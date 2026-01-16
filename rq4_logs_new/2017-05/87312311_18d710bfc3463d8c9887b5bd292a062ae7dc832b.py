import numpy as np
import spacy
import re
from keras.models import Sequential
from keras.layers import Dense, Activation, Dropout, Flatten
from keras.layers.convolutional import Conv1D
from keras.layers.pooling import MaxPooling1D
from keras.constraints import maxnorm

from base_model import BaseModel

nlp = spacy.load('en')


class Model(BaseModel):
    version = 1

    params = []

    #params = [
    #    {'opt': optimizers.SGD, 'epochs': 10, 'lr': 0.1, 'n_words': 3, 'hidden': [100]},
    #    {'opt': optimizers.SGD, 'epochs': 100, 'lr': 0.1, 'n_words': 3, 'hidden': [100]},
    #    {'opt': optimizers.SGD, 'epochs': 100, 'lr': 0.05, 'n_words': 3, 'hidden': [100]},
    #    {'opt': optimizers.SGD, 'epochs': 500, 'lr': 0.05, 'n_words': 3, 'hidden': [100]},
    #]

    # for epochs in [10, 100, 250]:
    #     for lr in [0.01, 0.05, 0.1]:
    #         for n_words in [2, 3, 5]:
    #            for l1 in [[10], [100], [1000]]:
    #                for l2 in [[], l1]:
    #                    params.append({'opt': 'SGD', 'epochs': epochs, 'lr': lr, 'n_words': n_words, 'hidden': l1 + l2})

    # for epochs in [250, 350]:
    #     for lr in [0.03]:
    #         for n_words in [10]:
    #            for l1 in [[1000], [1500], [2000]]:
    #                for nhidden in [2]:
    #                    params.append({'bs': 256, 'opt': 'SGD', 'epochs': epochs, 'lr': lr, 'n_words': n_words, 'hidden': l1 * nhidden})

    #params.append({'bs': 256, 'opt': 'SGD', 'epochs': 250, 'lr': 0.025, 'n_words': 5, 'hidden': [2000, 2000]})

    # params.append({"epochs": 250, "opt": "SGD", "bs": 256, "lr": 0.05, "n_words": 10, "hidden": [1000, 1000]})
    # params.append({"epochs": 250, "opt": "SGD", "bs": 256, "lr": 0.025, "n_words": 5, "hidden": [1000, 1000]})
    # params.append({"epochs": 250, "opt": "SGD", "bs": 256, "lr": 0.025, "n_words": 10, "hidden": [1000, 1000, 1000]})

    params.append({"epochs": 50, "opt": "SGD", "bs": 256, "lr": 0.25, "n_words": 10})
    #params.append({"epochs": 1, "opt": "SGD", "bs": 256, "lr": 0.025, "n_words": 10, "hidden": [1000, 1000, 1000]})


    def get_x(self, d):
        def get_row(row):
            name_re = re.compile(re.escape(row['name']), re.IGNORECASE)
            desc = name_re.sub("THENAME", row['description'])
            doc = nlp(desc)
            doclist = [w.string.strip() for w in doc]
            pad_vector = nlp('.').vector
            try:
                ind = doclist.index("THENAME")
            except ValueError:
                ind = -100

            out = []
            for i in list(range(ind - self.params['n_words'], ind)) + list(range(ind + 1, ind + self.params['n_words'] + 1)):
                if i < 0 or i >= len(doc):
                    out.append(pad_vector)
                else:
                    out.append(doc[i].vector)

            return out


        def get():
            return np.array([get_row(row) for index, row in d.iterrows()])

        k = "conv/x/n_words={}".format(self.params['n_words'])
        return self.cache.fetch(k, get)

    def create_model(self):
        model = Sequential()

        #model.add(Dropout(self.params.get('dropout', 0)))
        model.add(Conv1D(10, 2, input_shape=(20, 300, )))
        model.add(MaxPooling1D(2))
        model.add(Flatten())
        model.add(Dense(100, activation='relu')) # , kernel_constraint=maxnorm(3)
        model.add(Dense(self.n_out, activation='sigmoid'))

        return model