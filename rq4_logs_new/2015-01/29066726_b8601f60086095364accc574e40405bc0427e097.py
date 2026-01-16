import nltk
from nltk.corpus import wordnet as wn
from itertools import chain
from random import choice

#Fixes some holes in the WordNet mapping
custom_map = {'am':'am', 'create':'destroy', 'kill':'make_life', 'worship':'deface',
                'are':'are', 'be':'be', "is":"isn't", 'i am':'i am not'}
custom_map.update(reversed(i) for i in custom_map.items())

def anti_words(word):
    """Returns a list of antonym for the word"""
    if word in custom_map:
        return custom_map[word]
    ant_bag = set([word])
    for syn in wn.synsets(word):
        try:
            #print syn.lemmas()
            ant_bag.add(syn.lemmas()[0].antonyms()[0].name())
        except:
            ant_bag.add(word)

    if len(ant_bag) > 1:
        try:
            ant_bag.remove(word)
        except KeyError:
            pass
    return ant_bag

def anti_sentence(sentence):
    """A generator that chooses a random antonym for each word in the sentence and yields it"""
    words = sentence.split()
    for word in words:
        yield choice(tuple(anti_words(word)))

if __name__ == '__main__':
    while True:
        sen = raw_input(' : ')
        print ' '.join(list(anti_sentence(sen)))