"""
Final Project
-------------

Author: Dor Rondel
Course: CSc 11300
File: letter_frequency.py
"""

###################
### Global Vars ###
###################

TOTAL_CHARACTERS = 0
USED_CHARACTERS = ""

####################
# Module Functions #
####################

def letter_probability(text):
    ''' Determines frequency of letter by formula f = letter occurence / all letter occurence '''
    global TOTAL_CHARACTERS, USED_CHARACTERS
    for char in text.upper():
        if char not in USED_CHARACTERS:
            USED_CHARACTERS += char
        TOTAL_CHARACTERS += 1
    return [text.count(char) / TOTAL_CHARACTERS for char in USED_CHARACTERS]

def get_used():
    ''' returns used_characters string '''
    global USED_CHARACTERS
    return USED_CHARACTERS