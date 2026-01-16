import spacy

nlp = spacy.load('en_core_web_sm')
doc = nlp('Hello World!')

print('{0:10} {1:10} {2:10} {3:10} {4:10} {5:10} {6:10} {7:10}'.format('text', 'lemma', 'pos', 'tag', 'dep', 'shape', 'isalpha', 'isstop'))
for token in doc:
    print('{0:10} {1:10} {2:10} {3:10} {4:10} {5:10} {6:10} {7:10}'.format(token.text, token.lemma_, token.pos_, token.tag_, token.dep_, token.shape_, token.is_alpha, token.is_stop))