import tensorflow as tf
import gensim
import numpy as np
from gensim.models import word2vec
from gensim.models.word2vec import LineSentence

if __name__ == "__main__":
    sentences = LineSentence('../data/corpus.txt')

    model = word2vec.Word2Vec(sentences,max_vocab_size=7000)
    model.save("../data/model_w2v")
    # print(model["我"]-model["你"])
    pass