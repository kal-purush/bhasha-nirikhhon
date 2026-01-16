# Author: Alejandro Nolla - z0mbiehunt3r
# Purpose: Example for detecting language using a stopwords based approach
# Created: 15/05/13
# original code: http://blog.alejandronolla.com/2013/05/15/detecting-text-language-with-python-and-nltk/

# Edit: Athanasios Giannakopoulos
# Date: 13/01/2017

import sys, numpy
from nltk import wordpunct_tokenize
from nltk.corpus import stopwords


def _calculate_languages_ratios(text):
    """
    Calculate probability of given text to be written in several languages and
    return a dictionary that looks like {'french': 2, 'spanish': 4, 'english': 0}
    
    @param text: Text whose language want to be detected
    @type text: str
    
    @return: Dictionary with languages and unique stopwords seen in analyzed text
    @rtype: dict
    """

    languages_ratios = {}

    tokens = wordpunct_tokenize(text)
    words = [word.lower() for word in tokens]

    # Compute per language included in nltk number of unique stopwords appearing in analyzed text
    # choose only between 3 languages
    for language in ['english', 'german', 'french']:
        stopwords_set = set(stopwords.words(language))
        words_set = set(words)
        common_elements = words_set.intersection(stopwords_set)

        languages_ratios[language] = len(common_elements) # language "score"

    for value in languages_ratios.values():
        if value != 0:
            return languages_ratios
    
    return 'nan'

def detect_language(text):
    """
    Calculate probability of given text to be written in several languages and
    return the highest scored.
    
    It uses a stopwords based approach, counting how many unique stopwords
    are seen in analyzed text.
    
    @param text: Text whose language want to be detected
    @type text: str
    
    @return: Most scored language guessed
    @rtype: str
    """

    ratios = _calculate_languages_ratios(text)

    if ratios == 'nan':
        return numpy.nan

    most_rated_language = max(ratios, key=ratios.get)

    if most_rated_language == 'english':
        return 'en'
    if most_rated_language == 'german':
        return 'de'
    if most_rated_language == 'french':
        return 'fr'