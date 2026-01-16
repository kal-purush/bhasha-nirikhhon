#!/usr/bin/python
# -*-coding:utf-8-*-

import sys
from collections import defaultdict
import pickle

def wiki_count_pattern(ngram_data):
    count_instance = defaultdict(int)
    for line in open(ngram_data):
        line = line.strip()
        line_split = line.split("\t")
        instance = line_split[0]
        count    = int(line_split[-1])
        count_instance[instance] += count
    # f = open("/Users/hirataai/tensor/data/Wikipedia/data/wiki_noundict.dunp", "w")
    # pickle.dump(count_noun, f)
    
    for ins, count in sorted(count_instance.items(), key=lambda x:x[1], reverse=True):
        print "{}\t{}".format(ins, count)


if __name__ == "__main__":
    wiki_count_pattern(sys.argv[1])