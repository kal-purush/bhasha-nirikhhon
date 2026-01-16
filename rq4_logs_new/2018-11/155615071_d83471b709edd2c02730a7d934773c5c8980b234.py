from nltk.tokenize import word_tokenize
import re

class EntityRecognizer:
    
    def __init__(self):
        self._entities = ['UNK']
        self._finalized = False
        self._this_regex = re.compile('(^|\s)(t|T)his \w*')
        
    def get_entity(self, sentence):
        entity = word_tokenize(self._this_regex.search(sentence).group())[1]
        return entity
        
    def add_sentence(self, sentence, return_entity=False):
        assert not self._finalized, 'Vocab is already finalized'
        
        entity = self.get_entity(sentence)
        if entity not in self._entities:
            self._entities.append(entity)
        if return_entity:
            return entity

    def finalize_vocab(self):
        self._finalized = True
    
    def get_entity_vector(self, sentence):
        assert self._finalized, 'Vocab not finalized'
        sentence_entity = self.get_entity(sentence)
        if sentence_entity not in self._entities:
            return 'UNK'
        else:
            return sentence_entity
        
    def get_all_entities(self):
        return self._entities