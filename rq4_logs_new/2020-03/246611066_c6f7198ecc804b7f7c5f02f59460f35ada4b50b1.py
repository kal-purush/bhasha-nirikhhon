#!/usr/bin/env python
# coding: utf-8

# In[2]:


import nltk
#help(nltk.sent_tokenize)
with open("/home/lzanella/ameliepoulain.txt") as infile:
    content = infile.read()

sentences = nltk.sent_tokenize(content)
print(sentences)


# In[3]:


# Open the file "data/hp.txt"
with open("/home/lzanella/ameliepoulain.txt") as infile:
    content = infile.read()

# Use NLTK to tokenize) the text
tokens = nltk.word_tokenize(content)

# Print out the tokens
print(tokens)


# In[5]:


# Open and read in file as a string, assign it to the variable `content`
with open("/home/lzanella/ameliepoulain.txt") as infile:
    content = infile.read()
    
# Split up entire text into tokens using word_tokenize():
tokens = nltk.word_tokenize(content)

# create an empty list to collect all words ending in -ed:
ed_words = []

# Iterate over all tokens
for token in tokens:
    # check if a token ends with -ed
    if token.endswith("ed"):
        # if the condition is met, add it to the ed-list
        ed_words.append(token)
# Print the ed-words
print(ed_words)


# In[6]:


def segment_and_tokenize(string):
    # Sentence splitting
    content = nltk.sent_tokenize(string) 
    # tokenizing
    content = [nltk.word_tokenize(sentence) for sentence in content]  
    
    return content
    

with open("/home/lzanella/ameliepoulain.txt") as infile:
        #print(filename)
        string = infile.read()
        # Split and Tokenize
        out = segment_and_tokenize(string)

# Print out the result
print(out)


# In[7]:


# Define a translation table that maps each punctuation sign to the empty string
import string
translator = str.maketrans('', '', string.punctuation)

def remove_punctuation(string):
    tokens = segment_and_tokenize(string) 
    # remove punctuation  
    out = [[w.translate(translator) for w in l] for l in tokens]  
    
    return out

with open("/home/lzanella/ameliepoulain.txt") as infile:
        #print(filename)
        string = infile.read()
        # Segment, tokenize and remove punctuation
        out = remove_punctuation(string)
        
print(out)


# In[8]:


def lower_case(string):
    # Segment, tokenize, remove punctuation
    tokens = remove_punctuation(string)
    # Lowercase
    return [[w.lower() for w in l] for l in tokens] 

with open("/home/lzanella/ameliepoulain.txt") as infile:
        #print(filename)
        string = infile.read()
        #  Segment, tokenize, remove punctuation and lower case
        out = lower_case(string)
        
print(out)


# In[11]:


import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

def remove_stop_words(tokens):
    
    # Segment, tokenize, remove punctuation and lower case
    tokens = lower_case(string)

    # Remove stop words and return result
    return[[w for w in l  if w not in stopwords.words('english')] for l in tokens]

with open("/home/lzanella/ameliepoulain.txt") as infile:
        #print(filename)
        string = infile.read()
        # Sentence splitting
        out = remove_stop_words(string)
        
print(out)


# In[12]:


# Start by reading the documentation:
import nltk
help(nltk.pos_tag)


# In[14]:


def get_ed_verbs(text):

    nltk.download('averaged_perceptron_tagger')

    # Apply tokenization and POS tagging
    tokens = nltk.word_tokenize(content)
    tagged_tokens = nltk.pos_tag(tokens)

    # List of verb tags (i.e. tags we are interested in)
    verb_tags = ["VBD", "VBG", "VBN", "VBP", "VBZ"]

    # Create an empty list to collect all verbs:
    verbs = []

    # Iterating over all tagged tokens
    for token, tag in tagged_tokens:
 
        # Checking if the tag is any of the verb tags
        if tag in verb_tags and token.endswith('ed'):
            # if the condition is met, add it to the list we created above 
            verbs.append(token)
            
    return verbs
            
# Open and read in file as a string, assign it to the variable `content`
with open("/home/lzanella/ameliepoulain.txt") as infile:
    content = infile.read()
    ed_verbs = get_ed_verbs(content)
    print(ed_verbs)


# In[1]:


import spacy
import pandas as pd
# Load the SpaCy model for English
nlp = spacy.load('en')

# Define test sentence
sentence = "Amélie is a story about a girl named Amélie whose childhood was suppressed by her Father's mistaken concerns of a heart defect."
nlp_sentence = nlp(sentence)
spacy_pos_tagged = [(word, word.tag_, word.pos_) for word in nlp_sentence]

# Visualize results using Pandas datafram 
pd.DataFrame(spacy_pos_tagged, columns=['Word', 'POS tag', 'Tag type'])


# In[4]:


import nltk
nltk.download('wordnet')

sentence = "Amélie is a story about a girl named Amélie whose childhood was suppressed by her Father's mistaken concerns of a heart defect."

# Tokenize and Pos-tag the sentence
tokens = nltk.word_tokenize(sentence)
tagged_tokens = nltk.pos_tag(tokens)

# Specify which POS tags label verbs
verb_tags = ["VBD", "VBG", "VBN", "VBP", "VBZ"]

# Collect all verb tokens
verbs = []
for token, tag in tagged_tokens:
    if tag in verb_tags:
        verbs.append(token)
       
    
# Instantiate a lemmatizer object
lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()

# Create an empty list of verb lemmas:
verb_lemmas = []
        
for word_form in verbs:
    # For this lemmatizer, we need to indicate the POS of the word (in this case, v = verb)
    lemma = lemmatizer.lemmatize(word_form, "v") 
    verb_lemmas.append((word_form,lemma))

for (f,l) in verb_lemmas:
    print((f,l))


# In[7]:


# Lemmatizing (the proper way, accounting for different POS tags)
from nltk.corpus import wordnet as wn


# Write a  function to translate penn tree bank tags to wordnet tags
def penn_to_wn(penn_tag):
    """
    Returns the corresponding WordNet POS tag for a Penn TreeBank POS tag.
    """
    if penn_tag in ['NN', 'NNS', 'NNP', 'NNPS']:
        wn_tag = wn.NOUN
    elif penn_tag in ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']:
        wn_tag = wn.VERB
    elif penn_tag in ['RB', 'RBR', 'RBS']:
        wn_tag = wn.ADV
    elif penn_tag in ['JJ', 'JJR', 'JJS']:
        wn_tag = wn.ADJ
    else:
        wn_tag = None
    return wn_tag

# Create a lemmatizer instance
lmtzr = nltk.stem.wordnet.WordNetLemmatizer()

# create an empty list to collect lemmas
lemmas = []

# Iterate over the list of tagged tokens obtained before
for token, pos in tagged_tokens:
    # convert Penn Treebank POS tag to WordNet POS tag
    wn_tag = penn_to_wn(pos) 
    # Check if a wordnet tag was assigned
    if not wn_tag == None:
        # we lemmatize using the translated wordnet tag
        lemma = lmtzr.lemmatize(token, wn_tag)
    else:
        # if there is no wordnet tag, we apply default lemmatization
        lemma = lmtzr.lemmatize(token)
    # add lemmas to list
    lemmas.append(lemma)
    
# Inspect lemmas by printing them
print(lemmas)


# In[8]:


def lemmatize_postagged_tokens(tagged_tokens):
    
    # Create a lemmatizer instance
    lmtzr = nltk.stem.wordnet.WordNetLemmatizer()

    # create an empty list to collect lemmas
    lemmas = []
    
    # Iterate over the list of tagged tokens obtained before
    for token, pos in tagged_tokens:
        # convert Penn Treebank POS tag to WordNet POS tag
        wn_tag = penn_to_wn(pos) 
        # Check if a wordnet tag was assigned
        if not wn_tag == None:
            # we lemmatize using the translated wordnet tag
            lemma = lmtzr.lemmatize(token, wn_tag)
        else:
            # if there is no wordnet tag, we apply default lemmatization
            lemma = lmtzr.lemmatize(token)
        # add (token, lemma, pos) lemmas to list
        tlp = (token, lemma, pos)
        lemmas.append(tlp)
        
    return lemmas
    
# Lemmatize a list of tagged tokens
lemmatize_postagged_tokens(tagged_tokens)


# In[9]:


import spacy

# Initialize spacy 'en' model, keeping only tagger component needed for lemmatization
nlp = spacy.load('en', disable=['parser', 'ner'])

sentence = "The striped bats are hanging on their feet for best"

# Parse the sentence using the loaded 'en' model object `nlp`
doc = nlp(sentence)

# Extract the lemma for each token and join
" ".join([token.lemma_ for token in doc])


# In[34]:


import sys
import os
java_path = r'D:/jdk-13.0.2/bin/java.exe'
os.environ['JAVAHOME'] = java_path
from nltk.parse.stanford import StanfordParser
#from nltk.parse.corenlp import CoreNLPParser
scp = StanfordParser(path_to_jar='/home/lzanella/stanford-parser-full-2018-10-17/stanford-parser.jar',
           path_to_models_jar='/home/lzanella/stanford-parser-full-2018-10-17/stanford-parser-3.9.2-models.jar')

with open("/home/lzanella/ameliepoulain.txt", "r", encoding="utf8", errors='ignore') as infile:
    content = infile.read()
    sentences = nltk.sent_tokenize(content)                     # split the content into sentences
        
    counter=0
   
    for sentence in sentences[0:10]:
        print("\n SENTENCE %i : %s \n \n NPs: \n"%(counter,sentence))
        counter+=1      
        parse_trees = list(scp.raw_parse(sentence))
        tree = parse_trees[0]
        # get all NP trees and extract their leaves
        # Use help(nltk.tree.Tree) to find out which NLTK method you can use to do this
        for s in tree.subtrees(lambda tree: tree.label() == "NP"):
           print(s.leaves())
        


# In[29]:


# Named Entity Recognition (Using Stanford NLP)
from nltk.tag import StanfordNERTagger
import os
import pandas as pd
java_path = 'D:/jdk-13.0.2/bin/java.exe'
os.environ['JAVA_HOME'] = java_path
sner = StanfordNERTagger('/home/lzanella/stanford-ner-2018-10-16/classifiers/english.all.3class.distsim.crf.ser.gz',
                       path_to_jar='/home/lzanella/stanford-ner-2018-10-16/stanford-ner.jar')

named_entities = []

with open("/home/lzanella/ameliepoulain.txt", "r", encoding="utf8", errors='ignore') as infile:
    content = infile.read()
    sentences = nltk.sent_tokenize(content)                     
        
    counter=0
    
    for sentence in sentences[0:3]:
        # print("\n SENTENCE %i : %s \n \n NE: \n"%(counter,sentence))
        counter+=1
        ner_tagged_sentence = sner.tag(sentence.split())
        sentence_named_entities = [ne for ne in ner_tagged_sentence
                  if ne[1] != 'O']
        # named_entities.append(sentence_named_entities)
        named_entities = named_entities + sentence_named_entities
print(set(named_entities))


# In[16]:


# Named Entity Recognition (Using SPACY)
from nltk.tag import StanfordNERTagger
import spacy
from spacy import displacy
from pprint import pprint
nlp = spacy.load('en', entity=True)
nlp_sentence = nlp("Amélie is a story about a girl named Amélie whose childhood was suppressed by her Father's mistaken concerns of a heart defect.")

print("nlp_sentence")
displacy.render(nlp_sentence, style='ent', jupyter=True)


# In[ ]:



