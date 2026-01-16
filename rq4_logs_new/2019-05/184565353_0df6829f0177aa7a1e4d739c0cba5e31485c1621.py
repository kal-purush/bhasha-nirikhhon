import os
import pickle

import tensorflow as tf

from utils.utils import calc_num_batches


def load_vocab(vocab_fpath):
    '''Loads vocabulary file and returns idx<->token maps
    vocab_fpath: string. vocabulary file path.
    Note that these are reserved
    0: <s>, 1: <unk>, 2: <pad>, 3:</s>

    Returns
    two dictionaries.
    '''
    vocab = [line.split()[0] for line in open(vocab_fpath, 'r', encoding='utf-8').read().splitlines()]
    token2idx = {token: idx for idx, token in enumerate(vocab)}
    idx2token = {idx: token for idx, token in enumerate(vocab)}
    return token2idx, idx2token


def load_data(fpath):
    ''' Loads the preprocessed data pickle.
    This assumes the following:
    - image data is preprocessed with a VGGNet to (196,512) and this array is flattened with numpy
    - text data is preprocessed with SentencePiece and encoded as ids, and padded to the longest sentence

    Returns
    data: dict with key=image_id and value=(image encoded,caption encoded)
    '''

    data = pickle.load(open(fpath, 'rb'))
    return data


# def encode(inp, type, dict):
#     '''Converts string to number. Used for `generator_fn`.
#     inp: 1d byte array.
#     type: "x" (source side) or "y" (target side)
#     dict: token2idx dictionary
#
#     Returns
#     list of numbers
#     '''
#     inp_str = inp.decode("utf-8")
#     if type == "x":
#         tokens = inp_str.split() + ["</s>"]
#     else:
#         tokens = ["<s>"] + inp_str.split() + ["</s>"]
#
#     x = [dict.get(t, dict["<unk>"]) for t in tokens]
#     return x

def generator_fn(fpath, vocab_path):
    '''Generates training / evaluation data
    sents1: list of source sents
    sents2: list of target sents
    vocab_fpath: string. vocabulary file path.

    yields
    xs: tuple of
        id: picture_id
        x: array of flattened VGG output (192,512) flattened
    labels: tuple of
        id: picture_id
        y: list of token id of the caption (,max_length)

    '''
    # TODO: we could add the real sentence by decoding it here and put it in labels
    # _, idx2token = load_vocab(vocab_path)
    image_caption_dict = load_data(fpath)

    for value in image_caption_dict.values():
        uid, img, caption = value
        yield (uid, img), (uid, caption)


def input_fn(fpath, vocab_path, batch_size, shuffle):
    '''Batchify data
    image_caption_dict: dict of image_id -> (image_id, img_data, encoded caption)
    batch_size: scalar
    shuffle: boolean

    Returns
    xs: tuple of
        id: picture_id  -- (N,)
        x: array of flattened VGG output (192,512) flattened -- (N,192*512)
    labels: tuple of
        id: picture_id -- (N,)
        y: list of token id of the caption (,max_length) -- (N,max_length)
    '''
    shapes = (
        ((), [196 * 512]),
        ((), [None])  # Max length -- length of the longest encoded caption
    )
    types = (
        (tf.string, tf.float32),
        (tf.string, tf.float32)
    )

    dataset = tf.data.Dataset.from_generator(
        generator_fn,
        output_shapes=shapes,
        output_types=types,
        args=([fpath, vocab_path])  # <- arguments for generator_fn. converted to np string arrays
    )

    if shuffle:  # for training
        dataset = dataset.shuffle(128 * batch_size)

    dataset = dataset.repeat()  # iterate forever
    dataset = dataset.batch(batch_size).prefetch(1)

    return dataset


def get_batch(fpath, vocab_path, batch_size, shuffle=False):
    '''Gets training / evaluation mini-batches
    fpath: path to pickle of dataset
    batch_size: scalar
    shuffle: boolean

    Returns
    batches
    num_batches: number of mini-batches
    num_samples
    '''
    images_caption_dict = load_data(fpath)
    batches = input_fn(fpath, vocab_path, batch_size, shuffle=shuffle)
    num_batches = calc_num_batches(len(images_caption_dict), batch_size)
    return batches, num_batches, len(images_caption_dict)


if __name__ == '__main__':
    # Try to iterate over the dataset with tf.data.Dataset
    tf.enable_eager_execution()

    dir_path = os.getcwd()

    data = 'dataset/Flickr8k/prepro/train_set.pkl'
    vocab = 'dataset/Flickr8k/prepro/trained_sp.vocab'
    maxlen = 1000
    b_size = 128

    b, num_b, num_s = get_batch(data, vocab, b_size, shuffle=False)

    for val in b.take(1):
        tf.print(val)
        print(val)
