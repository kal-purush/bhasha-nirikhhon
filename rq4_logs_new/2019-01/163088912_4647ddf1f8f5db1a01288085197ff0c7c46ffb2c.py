"""Interact with a model"""

__author__ = "Guillaume Genthial"

from pathlib import Path
import functools
import json

import tensorflow as tf

from main import model_fn

root_dir = "/data/kongyy/nlp/tf_ner_guillaumegenthial/"
DATADIR = root_dir + 'example'
PARAMS = root_dir + 'results/params.json'
MODELDIR = root_dir + 'results/model'


def parse_fn(line_words):
    # Encode in Bytes for TF
    words = [w.encode() for w in line_words.strip().split()]
    return words, len(words)


def fwords(name):
    return str(Path(DATADIR, '{}.words.txt'.format(name)))


def input_fn(words, params=None, shuffle_and_repeat=False):
    params = params if params is not None else {}
    shapes = ([None], ())
    types = (tf.string, tf.int32)
    defaults = ('<pad>', 0)

    dataset = tf.data.Dataset.from_generator(
        functools.partial(generator_fn, words),
        output_shapes=shapes, output_types=types)

    if shuffle_and_repeat:
        dataset = dataset.shuffle(params['buffer']).repeat(params['epochs'])

    dataset = (dataset
               .padded_batch(params.get('batch_size', 20), shapes, defaults)
               .prefetch(1))
    return dataset


def generator_fn(words):
    with Path(words).open('r') as f_words:
        for line_words in f_words:
            yield parse_fn(line_words)


def write_predictions(name):
    with Path(root_dir + 'results/score/{}.preds.txt'.format(name)).open('wb') as f:
        test_inpf = functools.partial(input_fn, fwords(name))
        golds_gen = generator_fn(fwords(name))
        preds_gen = estimator.predict(test_inpf)
        for golds, preds in zip(golds_gen, preds_gen):
            ((words, _), _) = golds
            f.write(b" ".join(words) + b"\n")
            f.write(b" ".join(preds['tags']) + b"\n")
            f.write(b'\n')


if __name__ == '__main__':
    params = {
        'dim': 60,
        'dropout': 0.5,
        'num_oov_buckets': 1,
        'epochs': 1,
        'batch_size': 32,
        'buffer': 15000,
        'lstm_size': 100
    }

    params['words'] = str(Path(DATADIR, 'vocab.words.txt'))
    params['chars'] = str(Path(DATADIR, 'vocab.chars.txt'))
    params['tags'] = str(Path(DATADIR, 'vocab.tags.txt'))
    params['glove'] = str(Path(DATADIR, 'w2v.npz'))

    estimator = tf.estimator.Estimator(model_fn, MODELDIR, params=params)
    write_predictions("speed")