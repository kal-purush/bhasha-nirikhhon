#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import os
from random import uniform
from collections import defaultdict

DEBUG_PRINT = True
ORDER = 3
END_OF_SENT_SYM = ".,:;?!"
EMPTY_WORD = "$"
r_alphabet = re.compile(u'[ёa-zA-Zа-яА-Я0-9-]+|[' + END_OF_SENT_SYM + '\n]+')

def print_model(model):
    if DEBUG_PRINT:
        print "\n-------- model ---------------------------------------"
        print "previous words" + ('\t' * (ORDER - 2)) + "|  next word\t: next word probality"
        print "------------------------------------------------------"
        for prev_words, next_words_info in model.iteritems():
            str_next_word_info = ('\n' + ('\t' * ORDER)).join(next_word + "\t: " + str(probality) for (next_word,probality) in next_words_info)
            print "\t".join(prev_words) + "\t|\t" + str_next_word_info

def get_table_header(words_count):
    str = ""
    for i in range(words_count):
        str += ( "wrd %s\t" % (i))
    return str

def gen_lines(corpus):
    data = open(corpus)
    for line in data:
        yield line.decode('utf-8').lower()

def gen_tokens(lines):
    for line in lines:
        for token in r_alphabet.findall(line):
            yield token.encode('utf-8')

def gen_n_grams(tokens,order):
    tn = [EMPTY_WORD for x in range(order - 1)]
    for token in tokens:
        yield tn + [token]
        if token in END_OF_SENT_SYM:
            tn.append(token)
            for i in range(order - 1):
                tn.append(EMPTY_WORD)
                tn.pop(0)
                yield tn
            tn.pop(0)
        else:
            tn.append(token)
            tn.pop(0)

def train(corpus,model):
    lines = gen_lines(corpus)
    tokens = gen_tokens(lines)
    n_grams = gen_n_grams(tokens,ORDER)

    n_min_stat = defaultdict(lambda: 0.0) # number of previous word chains N-1
    n_stat = defaultdict(lambda: 0.0) # number of previous word chains N

    if DEBUG_PRINT:
        print "GENERATING " + str(ORDER) + "-gram"
        print "\n-------- n_grams --------"
        print get_table_header(ORDER)
        print "--------------------------"

    for n_gram in n_grams:
        if DEBUG_PRINT:
            print "\t".join(n_gram)
        n_min_stat[tuple(n_gram[:-1])] += 1 # calculate freq for word chain
        n_stat[tuple(n_gram)] += 1 # calculate freq for word chain

    if DEBUG_PRINT:
        print "\nCONVERTING TO DICT WITH WEIGS"
        print "\n-------- n_min_stat with weigs ---------"
        print get_table_header(ORDER - 1) + ": weigh\t"
        print "----------------------------------"
        for words, freq in n_min_stat.iteritems():
            print "%s\t: %s" % ("\t".join(words), freq)
        print "\n-------- n_stat with weigs --------"
        print get_table_header(ORDER) + ": weigh\t"
        print "----------------------------------"
        for words, freq in n_stat.iteritems():
            print "%s\t: %s" % ("\t".join(words), freq)

    for words, freq in n_stat.iteritems():
        prev_words = words[:-1]
        next_word = words[-1]
        if prev_words in model:
            model[prev_words].append((next_word, freq/n_min_stat[prev_words]))
        else:
            model[prev_words] = [(next_word, freq/n_min_stat[prev_words])]

    print_model(model)

    return model

def generate_sentence(model):

    if DEBUG_PRINT:
        print "\nGENERATING SENTENCE"
    phrase = ''
    prev_words = [EMPTY_WORD] * (ORDER - 1)
    while 1:
        if DEBUG_PRINT:
            print "PREV WORD:\t" + "\t".join(prev_words) + "\tAVAILABLE NEXT WORDS: %s" % str(model[tuple(prev_words)])
        prev_words.append(unirand(model[tuple(prev_words)]))
        prev_words.pop(0)
        next_word = prev_words[-1]
        if DEBUG_PRINT:
            print "SELECTED WORD:\t" + next_word
        if next_word == EMPTY_WORD: break
        if next_word in (END_OF_SENT_SYM) or prev_words[-1] == EMPTY_WORD:
            phrase += next_word
        else:
            phrase += ' ' + next_word
    return phrase.capitalize()

def unirand(seq): # возвращает случайное слово с вероятностью, равной вероятности данного слова в зависимости от двух предыдущих
    sum_, freq_ = 0, 0
    for item, freq in seq:
        sum_ += freq
    rnd = uniform(0, sum_)
    for token, freq in seq:
        freq_ += freq
        if rnd < freq_:
            return token



def save_model(model,file_name):
    with open(file_name, "w") as csv_file:
        writer = csv.writer(csv_file, dialect='excel')
        for prev_words, next_words_info in model.iteritems():
            for (next_word,probality) in next_words_info:
                csv_row = list(prev_words)
                csv_row.extend([next_word,str(probality)])
                writer.writerow(csv_row)

def load_model(file_name):
    model = {}

    if not os.path.exists(file_name):
        return model

    with open(file_name, "rb") as csv_file:
        reader = csv.reader(csv_file, dialect='excel')
        for row in reader:
            prev_words = tuple(row[:-2])
            next_words_info = (row[-2],float(row[-1]))
            if prev_words in model:
                model[prev_words].append(next_words_info)
            else:
                model[prev_words] = [next_words_info]
        print_model(model)
    
    return model




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Markov model order: "+ str(ORDER) +" Sentences generator")
    parser.add_argument('--train_text_file', metavar='path', required=False, help='the path to text file with train data')
    parser.add_argument('--model_file', metavar='path', required=False, help='the path to csv file with pretrained model')
    parser.add_argument('--num_of_sent', metavar='N', type=int, required=False, help='number of sentences')
    parser.add_argument('--logs', action='store_true')
    args = parser.parse_args()

    DEBUG_PRINT = args.logs

    model = None
    if args.model_file is not None:
        model = load_model(args.model_file)

    if args.train_text_file is not None:
        if model is None:
            model = {}
        model = train(args.train_text_file,model)

    if model is None:
        print "model is empty \nusage: \npython model.py --model_file dump_model.csv --train_text_file input.txt --num_of_sent 3 --logs"
        exit()
    
    if args.model_file is not None:
        save_model(model, args.model_file)


    sentences_count = int(args.num_of_sent) if args.num_of_sent is not None else 1
    result = ""
    for i in range(sentences_count):
        result += generate_sentence(model)
    print result
