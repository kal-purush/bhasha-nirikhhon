'''Given a string, return the dictionary of words and number of occurences'''
from pyproblems.utility import is_str

def word_count(s_param):
    '''Return a dictionary of words with number of occurences of each word'''

    if not is_str(s_param):
        raise TypeError(f"Unsupported Type {type(s_param)}")

    words = s_param.split()
    word_dict = dict()

    for word in words:
        if word in word_dict:
            word_dict[word] += 1
        else:
            word_dict[word] = 1

    return word_dict
    