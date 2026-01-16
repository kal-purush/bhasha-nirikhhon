import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
import os
from gensim import corpora
import gensim

import tempfile
# TEMP_FOLDER = tempfile.gettempdir()
# print('Folder "{}" will be used to save temporary dictionary and corpus.'.format(TEMP_FOLDER))
#

# documents = ["Human machine interface for lab abc computer applications",
#              "A survey of user opinion of computer system response time",
#              "The EPS user interface management system",
#              "System and human system engineering testing of EPS",
#              "Relation of user perceived response time to error measurement",
#              "The generation of random binary unordered trees",
#              "The intersection graph of paths in trees",
#              "Graph minors IV Widths of trees and well quasi ordering",
#              "Graph minors A survey"]
#
# # remove common words and tokenize
# stoplist = set('for a of the and to in'.split())
# texts = [[word for word in document.lower().split() if word not in stoplist]
#          for document in documents]
#
# # remove words that appear only once
# from collections import defaultdict
# frequency = defaultdict(int)
# for text in texts:
#     for token in text:
#         frequency[token] += 1
#
# texts = [[token for token in text if frequency[token] > 1] for text in texts]
#
# from pprint import pprint  # pretty-printer
# pprint(texts)

class MySentences(object):
    def __init__(self, filename):
        self.filename = filename

    def __iter__(self):
        for sentence in open(self.filename):
            yield sentence

def generate_sentences(filename,out_path):
    c = 0
    with open(out_path, 'w') as outfile:
        for line in open(filename):
            sentences = sent_tokenize(line)
            sentences = [word_tokenize(x.lower()) for x in sentences]
            for sentence in sentences:
                c +=1
                outfile.write(' '.join(sentence )+ '\n')
    print('Total number of sentences: %d ' % c)

def extract_title_abstract(in_path):
    out_path = os.path.join(os.path.dirname(os.path.abspath(in_path)), 'acm_title_abstract.txt')
    title_prefix= '#*'
    abstract_prefix ='#!'
    prev_prefix=''
    total = 0
    count_with_abstracts = 0
    with open(in_path, 'r') as f:
        with open(out_path,'w') as outfile:
            for line in iter(f):
                line = line.rstrip('\n')
                if line.startswith(title_prefix):
                    if prev_prefix == title_prefix:
                        outfile.write(title+'\n')
                    elif prev_prefix == abstract_prefix:
                        outfile.write(' '.join([title,abstract])+'\n')
                        count_with_abstracts +=1
                    total +=1
                    title = line[2:]
                    prev_prefix = title_prefix
                if line.startswith(abstract_prefix):
                    abstract=line[2:]
                    prev_prefix = abstract_prefix
            # write the last read title/abstract
            if prev_prefix == title_prefix:
                outfile.write(title)
            elif prev_prefix == abstract_prefix:
                outfile.write(' '.join([title, abstract]))
                count_with_abstracts += 1
    print("Writing file %s" % out_path)
    print('Total number of papers %d' %total)
    print('Papers that don\'t have abstract %d' % count_with_abstracts)
def main():

    acm_file = '/home/zwanli/data/acm.txt'
    acm_title_abstract_file = '/home/zwanli/data/acm_title_abstract.txt'
    if not os.path.exists(acm_title_abstract_file ):
        extract_title_abstract(acm_file)
    acm_sentences_file='/home/zwanli/data/acm_sentences.txt'
    generate_sentences(acm_title_abstract_file,acm_sentences_file)
    sentences = MySentences(acm_title_abstract_file)
    # model = gensim.models.Word2Vec(sentences,iter=1,min_count=5,workers=7,sg=0,size=200)

    a =1

if __name__ == '__main__':
    main()


