import re
from collections import Counter
import numpy as np
import pandas as pd
from utils import extract_featuers

nltk.download('twitter_samples')
nltk.download('stopwords')

#@title Q1 Process Data

def process_data(file_name):
    """
    Input:
        A file_name which is found in your current directory. You just have to read it in.
    Output:
        wordprobs: a dictionary where keys are all the processed lowercase words and the 
             values are the frequency that it occurs in the corpus (text file you read).
    """
    words = [] # return this variable correctly

    ### START CODE HERE ###

    #Open the file, read its contents into a string variable
    data = None

    # convert all letters to lower case
    lower_case_data = None

    # with words as keys, ensure that we have a count of their occurrences
    words = dict()

    ### END CODE HERE ###

    return words

#@title Q2: N-gram most likely next words

def probable_substitutes(word, probs, maxret = 10):
    """
    Input:
        word - The misspelled word
        probs - A dictionary of word --> prob
        maxret - Maximum number of words to return
    Returns:
        Tuples of the words and their probabilities, ordered by highest frequency.
        [(word1, prob1), ... ]
    """
    return [("hello", 0.5), ("world", )]


#@title Q3: The Minimum Edit Distnance

def min_edit_distance(source, target, ins_cost = 1, 
                      del_cost = 1, rep_cost = 2):
    '''
    Input:
        source: starting string
        target: ending string
        ins_cost: integer representing insert cost
        del_cost: integer representing delete cost
        rep_cost: integer representing replace cost
    Output:
        D: matrix of size (len(source)+1 , len(target)+1) 
           with minimum edit distances
        med: the minimum edit distance required to convert
             source to target
    '''
    <YOUR-CODE-HERE>
    return D, med