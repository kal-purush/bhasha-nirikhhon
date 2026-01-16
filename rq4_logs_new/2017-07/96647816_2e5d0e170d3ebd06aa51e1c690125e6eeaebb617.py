"""PURPOSE: this file provides some basic use cases for the nltk package.
The text analysis process can generally be divided into 2 steps. I'll
name them the preprocessing step and the processing step. The Preprocessing
step refers to the tokenization of the input text sequence. In short, we
need to break down the whole sequence into meaningful units (words) for
further processing. The processing step includes the word-level part of
speech tagging, the syntactic level parsing, and the understanding of the
semantics.

DATA: I scraped the wiki page of Dark Souls on 2017-07-09. This data is
split into 2 parts (darksouls_training.txt and darksouls_test.txt). I use
dark_souls_training.txt for this demo.
"""

# I. tokenization
# generally speaking there are 2 types of tokenization: at the setence level
# and at the word level. We are interested in the word level one here. But I
# will demostrate both here.
# 1. sentence_tokenize
from nltk.tokenize import sent_tokenize


with open('darksouls_training.txt', 'r') as fhandler:
    str_input  = fhandler.read()
sent_result = sent_tokenize(str_input)
print 'There are {num} sentences in the training data.'.format(num=len(sent_result))

# 2 word_tokenize
from nltk.tokenize import word_tokenize
token_result = word_tokenize(sent_result[0])
print token_result


# II. POS
# after breaking down the sentence into tokens, we want to call a POS-tagger
# to determin what POS each token/word is. There are many different taggers
# to choose from. The in nltk, the state-of-art tagger should be the
# CRFTagger. We can train our own tagger if we have enough labeled data. For
# now I will use the default pos_tag method
from nltk import pos_tag


pos_crf = pos_tag(token_result)
print pos_crf

# if you don't know what each symbol mean, you can call the following function to help. Let's check JJ for example
from nltk.help import upenn_tagset
print upenn_tagset('JJ')

# 3. syntactic parsing.
# TODO: write a demo