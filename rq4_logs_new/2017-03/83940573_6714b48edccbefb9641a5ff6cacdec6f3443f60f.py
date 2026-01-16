from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
from tensorflow.python.platform import gfile

# Special vocabulary symbols - we always put them at the start.
_PAD = b"_PAD"
_SOS = b"_SOS"
_EOS = b"_EOS"
_UNK = b"_UNK"
_START_VOCAB = [_PAD, _SOS, _EOS, _UNK]

PAD_ID = 0
SOS_ID = 1
EOS_ID = 2
UNK_ID = 3

data_dir = "/data/"
train_path = "/data/train"
valid_path = "/data/valid"
max_vocabulary_size = 40000

def create_vocabulary(vocab_path, data_paths, max_vocabulary_size):
    if not gfile.Exists(vocab_path):
        vocab = {}
        for path in data_paths:
            with gfile.GFile(path, mode="rb") as file:
                counter = 0
                for line in file:
                    counter += 1
                    tokens = list(line.strip())
                    for word in tokens:
                        if word in vocab:
                            vocab[word] += 1
                        else:
                            vocab[word] = 1
        vocab_list = _START_VOCAB + sorted(vocab, key=vocab.get, reverse=True)
        if len(vocab_list) > max_vocabulary_size:
            vocab_list = vocab_list[:max_vocabulary_size]
        with gfile.GFile(vocab_path, mode="wb") as vocab_file:
            for word in vocab_list:
                vocab_file.write(word + b"\n")

def initialize_vocabulary(vocab_path):
    if gfile.Exists(vocab_path):
        rev_vocab = []
        with gfile.GFile(vocab_path, mode="rb") as f:
            rev_vocab.extend(f.readlines())
        rev_vocab = [line.strip('\n') for line in rev_vocab]
        vocab = dict([(x, y) for (y, x) in enumerate(rev_vocab)])
        return vocab, rev_vocab

def sentence_to_ids(sentence, vocabulary):
    words = list(sentence.strip())
    return  [vocabulary.get(w, UNK_ID) for w in words]

def data_processing(data_path, target_path, vocab_path):
    if not gfile.Exists(target_path):
        vocab, _ = initialize_vocabulary(vocab_path)
        with gfile.GFile(data_path, mode="rb") as data_file:
            with gfile.GFile(target_path, mode="wb") as ids_file:
                for line in data_file:
                    ids = sentence_to_ids(line, vocab)
                    ids_file.write(" ".join([str(tok) for tok in ids]) + "\n")

if __name__ == "__main__":
    vocab_path = os.path.join(data_dir, "vocab.dat")
    create_vocabulary(vocab_path,[train_path + ".x.txt", train_path + ".y.txt"], max_vocabulary_size)
    x_train_ids_path = train_path + ".ids.x"
    y_train_ids_path = train_path + ".ids.y"
    data_processing(train_path + ".x.txt", x_train_ids_path, vocab_path)
    data_processing(train_path + ".y.txt", y_train_ids_path, vocab_path)
    x_valid_ids_path = valid_path + ".ids.x"
    y_valid_ids_path = valid_path + ".ids.y"
    data_processing(valid_path + ".x.txt", x_valid_ids_path, vocab_path)
    data_processing(valid_path + ".y.txt", y_valid_ids_path, vocab_path)