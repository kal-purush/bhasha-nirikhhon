import os
import numpy as np

# indentify all uniuque characters then encode their positions in dicts
def make_dicts(path=os.path.join("data", "processed", "corpus.txt")):
    text = open(path).read().lower()
    chars = set(text)
    print('total chars:', len(chars))
    char_indices = dict((c, i) for i, c in enumerate(chars))
    indices_char = dict((i, c) for i, c in enumerate(chars))
    return chars, char_indices, indices_char

# cut the text in semi-redundant sequences of maxlen characters
def make_sequences(maxlen, step, path=os.path.join("data", "processed", "corpus.txt")):
    text = open(path).read().lower()
    sentences = []
    next_chars = []
    for i in range(0, len(text) - maxlen, step):
        sentences.append(text[i: i + maxlen])
        next_chars.append(text[i + maxlen])
    print('nb sequences:', len(sentences))
    return sentences, next_chars

def vectorize(sentences,next_chars, chars, maxlen, char_indices):
    print('Vectorization...')
    X = np.zeros((len(sentences), maxlen, len(chars)), dtype=np.bool)
    y = np.zeros((len(sentences), len(chars)), dtype=np.bool)
    for i, sentence in enumerate(sentences):
        for t, char in enumerate(sentence):
            X[i, t, char_indices[char]] = 1
            y[i, char_indices[next_chars[i]]] = 1
    np.save(os.path.join("data", "train", "X"), X)
    np.save(os.path.join("data", "train", "y"), y)