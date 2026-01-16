#!/usr/bin/env python3

"""Generate random entries"""

import random, string
import sentence_generator as markov
from os import path

"""Resolve filename"""
def relative_filename(filename):
    return path.join(path.dirname(__file__), filename)

markov.buildMapping(markov.wordlist(relative_filename('portrait-of-the-artist.txt')), 1)

def random_string(n):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

def get_random_article():
    keywords = [random_string(3) for i in range(random.randrange(1, 4))]
    s = ''
    s += '---\n'
    s += 'keywords: [' + ', '.join(keywords) + ']\n'
    s += '---\n\n'
    for _ in range(random.randrange(4, 20)):
        s += markov.genSentence(1)
        s += ' '
    s += '\n'
    return s

if __name__ == "__main__":
    for i in range(10000):
        sentence = markov.genSentence(1)
        filename = ' '.join(sentence.split()[:4])
        filename = filename.replace('.', '')
        filename = filename.replace(',', '')
        with open('entries/' + filename + '.md', 'w') as f:
            f.write(get_random_article())