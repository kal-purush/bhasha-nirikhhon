import os
import csv
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
import argparse
import ntpath
import sys
import tensorflow as tf
import numpy as np

vocab = {}

unknown_words = {}

filtered_vocabulary = {}

glove_vocab = []
glove_word_to_id = {}
glove_embeddings = []
avg_num_embedd = []

MIN_WORD_FREQUENCY = 5

numbers_freq = 0



def load_glove_embeddings(embed_dir, embed_dim, keep_embed = False):
    # Load pre-trained embeddings
    f = open(os.path.join(embed_dir, 'glove.6B.{0}d.txt'.format(embed_dim)))
    print("Loading GloVe embedding data  " + str(f.name))
    #Count the number of the occurance of numbers in glove embeddings
    num_freq = 0
    global avg_num_embedd
    avg_num_embedd = np.zeros(200)
    for line in f:
        row = line.split()
        word = row[0]
        # Add the word to the vocabulary list of the pre-trained word embeddings dataset
        glove_vocab.append(word)
        if keep_embed:
            if is_number(word):
                num_freq +=1
                avg_num_embedd += [float(val) for val in row[1:]]
        # Add the embedding of the word
            glove_embeddings.append([float(val) for val in row[1:]])
    f.close()
    #calculate the number embedding as the avg of the numbers that are in glove embeddings
    #It's a naive way to do it!!
    avg_num_embedd = avg_num_embedd / num_freq
    # Create a word to id index
    global glove_word_to_id
    glove_word_to_id = {word: i for i, word in enumerate(glove_vocab)}

    return glove_vocab, glove_word_to_id, glove_embeddings


def add_unkonwn_word(word):
    if word in unknown_words:
        unknown_words[word] += 1
    else:
        unknown_words[word] = 1


def in_glove_vocab(word):
    """
    Check if the word id exists in the vocabulary of the pre-trained embeddings .
        :returns: Word id
        :rtype int
    """
    if word in glove_word_to_id:
        return True
    else:
        add_unkonwn_word(word)
        return False


def add_word(word, vocab):
    if word in vocab:
       vocab[word] += 1
    else:
        vocab[word] = 1
    return word

def is_number(s):
    try:
        float(s)
        global  numbers_freq
        numbers_freq += 1
        return True
    except ValueError:
        return False

def process_word(word):
    '''
    replace unknown words with 'unk' token, replace numbers with '<NUM>'. Otherwise, return word
    :param word:
    :return:
    '''
    if is_number(word):
        add_word('<NUM>',filtered_vocabulary)
        return '<NUM>'
    if in_glove_vocab(word):
        add_word(word, filtered_vocabulary)
        return word
    add_word('unk', filtered_vocabulary)
    return 'unk'


def is_less_frequent(word,threshold):
    if is_number(word) or word == 'unk' or vocab[word] > threshold:
        return False
    return True


def process_documents(path, dataset):
    """
     Parses paper raw data
     :return: A tuple of Papers' labels and abstracts, where abstracts are returned as strings
     """

    delimiter = ','
    if dataset == 'citeulike-t':
        delimiter = '\t'
    # read raw data
    print('Reading documents ...')
    with open(path, "r", encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f, delimiter=delimiter)
        first_line = True
        documents = {}
        row_length = 0
        for line in reader:
            if first_line:
                labels = line
                row_length = len(line)
                first_line = False
                continue
            doc_id = line[0]
            if dataset == 'citeulike-t':
                paper = line[1]
            elif row_length > 2:
                paper = line[1]+' '+line[4]
            sentences = sent_tokenize(paper)
            sentences = [word_tokenize(x) for x in sentences]
            documents[doc_id] = [add_word(word,vocab) for sentence in sentences for word in sentence]

    # process raw data,
    # - remove less frequent words
    # - replace unknown words with 'unk' token, replace numbers with '<NUM>'
    print('Processing documents ...')
    filtered_documents={}
    for doc_id in documents:
        filtered_documents[doc_id] =[]
        for word in documents[doc_id]:
            if not is_less_frequent(word, MIN_WORD_FREQUENCY):
                filtered_documents[doc_id].append(process_word(word))

    # write the processd documents
    parent_dir, filename = ntpath.split(path)
    out_path = os.path.join(parent_dir,'processed-'+filename)
    print('Writing processed file {0} ...'.format(out_path))
    with open(out_path, "w", encoding='utf-8', errors='ignore') as f:
        writer = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['doc_id','title+abstract'])
        for doc_id in filtered_documents:
            writer.writerow([doc_id]+[' '.join(filtered_documents[doc_id])])
    print('Finished Writing.'.format(out_path))


def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

def _bytes_feature(value):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def save_embeddings(filename):
    if bool(filtered_vocabulary):
        print('Writing embeddings data {0} ...'.format(filename))
        writer = tf.python_io.TFRecordWriter(filename)
        for word in filtered_vocabulary:
            # Create a feature
            if word == '<NUM>':
                word_id = -1
                word_embed = avg_num_embedd
            else:
                word_id = glove_word_to_id[word]
                word_embed = glove_embeddings[word_id]

            feature = {'word': _bytes_feature(tf.compat.as_bytes(word)),
                       'glove_id': _int64_feature(word_id),
                       'embed': tf.train.Feature(float_list=tf.train.FloatList(value=word_embed))}
            # Create an example protocol buffer
            example = tf.train.Example(features=tf.train.Features(feature=feature))
            # Serialize to string and write on the file
            writer.write(example.SerializeToString())
        writer.close()
        sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_dir', type=str, default='/home/wanli/data/Extended_ctr',
                        help='data directory containing input.txt')
    parser.add_argument("--dataset", "-d", type=str, default='dummy',
                        help="Which dataset to use", choices=['dummy', 'citeulike-a', 'citeulike-t'])
    parser.add_argument('--embedding_dir', type=str, default='/home/wanli/data/glove.6B/',
                        help='GloVe embedding directory containing embeddings file')
    parser.add_argument('--embedding_dim', type=int, default=200,
                        help='dimension of the embeddings', choices=['50', '100', '200', '300'])
    args = parser.parse_args()

    if args.dataset == 'citeulike-a':
        dataset_folder = args.data_dir + '/citeulike_a_extended'
    elif args.dataset == 'citeulike-t':
        dataset_folder = args.data_dir + '/citeulike_t_extended'
    elif args.dataset == 'dummy':
        dataset_folder = args.data_dir + '/dummy'
    else:
        print("Warning: Given dataset not known, setting to dummy")
        dataset_folder = args.data_dir + '/citeulike_a_extended'

    raw_data_path = os.path.join(dataset_folder, 'raw-data.csv')
    if not os.path.exists(raw_data_path):
        print("File {0} doesn't exist".format(raw_data_path))
        raise

    load_glove_embeddings(args.embedding_dir,args.embedding_dim,keep_embed=True)

    process_documents(raw_data_path,args.dataset)

    embeddings_path = os.path.join(dataset_folder, '{0}-embeddings-{1}.tfrecord'.format(args.dataset,args.embedding_dim))
    save_embeddings(embeddings_path)

    print('Raw data vocabulary size {0}'.format(len(vocab)))
    print('Processed data vocabulary size {0}'.format(len(vocab)-len(unknown_words)-numbers_freq))
    print('# of unique unknown words {0}, frequency of unknown words {1}'
          .format(len(unknown_words),sum(unknown_words.values())))
    print('Numbers frequency {0}'.format(numbers_freq))


if __name__ == '__main__':
    main()