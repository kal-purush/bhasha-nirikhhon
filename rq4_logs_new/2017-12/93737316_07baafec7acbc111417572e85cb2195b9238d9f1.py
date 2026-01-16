# this contains a single instance of the spacy NLP
# engine. This is used to guarantee a single global
# instance. If you declared a different engine for every
# DocumentCollection you created, you would run out of memory!
import spacy

GLOBAL_NLP_ENGINE = spacy.load('en')    # use English