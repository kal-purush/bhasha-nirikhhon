'''Assignment 2

Please add your code where indicated. You may conduct a superficial test of
your code by executing this file in a python interpreter.

The documentation strings ("docstrings") for each function tells you what the
function should accomplish. If docstrings are unfamiliar to you, consult the
Python Tutorial section on "Documentation Strings".

'''

def argmax(sequence):
    """Return the index of the highest value in a list.

    This is a warmup exercise.

    Remember that Python uses zero-based numbering of indexes.

    Args:
        sequence (list): A list of numeric values.

    Returns:
        int: The index of the highest value in `sequence`.

    """
    # YOUR CODE HERE



def tokenize(string, lowercase=False):
    """Extract words from a string containing English words.

    Handling of hyphenation, contractions, and numbers is left to your
    discretion.

    Tip: you may want to look into the `re` module.

    Args:
        string (str): A string containing English.
        lowercase (bool, optional): Convert words to lowercase.

    Returns:
        list: A list of words.

    """
    # YOUR CODE HERE


def shared_words(text1, text2):
    """Identify shared words in two texts written in English.

    Your function must make use of the `tokenize` function above. You should
    considering using Python `set`s to solve the problem.

    Args:
        text1 (str): A string containing English.
        text2 (str): A string containing English.

    Returns:
        set: A set with words appearing in both `text1` and `text2`.

    """
    # YOUR CODE HERE


def shared_words_from_filenames(filename1, filename2):
    """Identify shared words in two texts stored on disk.

    Your function must make use of the `tokenize` function above. You should
    considering using Python `set`s to solve the problem.

    For each filename you will need to `open` file and read the file's
    contents.

    There are two sample text files in the `data/` directory which you can use
    to practice on.

    Args:
        filename1 (str): A string containing English.
        filename2 (str): A string containing English.

    Returns:
        set: A set with words appearing in both texts.

    """
    # YOUR CODE HERE


def text2wordfreq(string, lowercase=False):
    """Calculate word frequencies for a text written in English.

    Handling of hyphenation and contractions is left to your discretion.

    Your function must make use of the `tokenize` function above.

    Args:
        string (str): A string containing English.
        lowercase (bool, optional): Convert words to lowercase before calculating their
            frequency.

    Returns:
        dict: A dictionary with words as keys and frequencies as values.

    """
    # YOUR CODE HERE


def lexical_density(string):
    """Calculate the lexical density of a string containing English words.

    The lexical density of a sequence is defined to be the number of
    unique words divided by the number of total words. The lexical
    density of the sentence "The dog ate the hat." is 4/5.

    Ignore capitalization. For example, "The" should be counted as the same
    type as "the".

    This function should use the `text2wordfreq` function.

    Args:
        string (str): A string containing English.

    Returns:
        float: Lexical density.

    """
    # YOUR CODE HERE



def hashtags(string):
    """Extract hashtags from a string.

    For example, the string `"RT @HouseGOP: The #StateOfTheUnion is strong."`
    contains the hashtag `#StateOfTheUnion`.

    Args:
        string (str): A string containing English.

    Returns:
        list: A list, possibly empty, containing hashtags.

    """
    # YOUR CODE HERE


def jaccard_similarity(text1, text2):
    """Calculate Jaccard Similarity between two texts.

    The Jaccard similarity (coefficient) or Jaccard index is defined to be the
    ratio between the size of the intersection between two sets and the size of
    the union between two sets. In this case, the two sets we consider are the
    set of words extracted from `text1` and `text2` respectively.

    This function should ignore capitalization. A word with a capital
    letter should be treated the same as a word without a capital letter.

    Args:
        text1 (str): A string containing English words.
        text2 (str): A string containing English words.

    Returns:
        float: Jaccard similarity

    """
    # YOUR CODE HERE






# DO NOT EDIT CODE BELOW THIS LINE

import unittest

import numpy as np


class TestAssignment2(unittest.TestCase):

    def test_argmax(self):
        self.assertEqual(argmax([0, 1, 2]), 2)
        self.assertEqual(argmax([3, 1, 2]), 0)

    def test_tokenize(self):
        words = tokenize('Colorless green ideas sleep furiously.', True)
        self.assertIn('green', words)
        self.assertIn('colorless', words)
        words = tokenize('The rain  in spain is  mainly in the plain.', False)
        self.assertIn('The', words)
        self.assertIn('rain', words)

    def test_text2wordfreq(self):
        counts = text2wordfreq('Colorless green ideas sleep furiously. Green ideas in trees.', True)
        self.assertEqual(counts['green'], 2)
        self.assertEqual(counts['sleep'], 1)
        self.assertIn('colorless', counts)
        self.assertNotIn('hello', counts)

    def test_shared_words(self):
        self.assertEqual(shared_words('the hat', 'red hat'), {'hat'})

    def test_shared_words_from_filenames(self):
        # the use of the os.path functions is required so that filenames work
        # on Windows and Unix/Linux systems.
        data_dir = os.path.join(os.path.dirname(__file__), 'data')
        filename1 = os.path.join(data_dir, '1984-chp01.txt')
        filename2 = os.path.join(data_dir, 'animal-farm-chp01.txt')
        words = shared_words_from_filenames(filename1, filename2)
        self.assertGreater(len(words), 3)
        self.assertIn('already', words)

    def test_lexical_density(self):
        self.assertAlmostEqual(lexical_density("The cat"), 1)
        self.assertAlmostEqual(lexical_density("The cat in the hat."), 4/5)

        tweet = """RT @HouseGOP: The #StateOfTheUnion isn't strong for the 8.7 million Americans out of work. #SOTU http://t.co/aa7FWRCdyn"""
        self.assertEqual(len(hashtags(tweet)), 2)

    def test_jaccard_similarity(self):
        text1 = "Eight million Americans"
        text2 = "Americans in the South"
        self.assertAlmostEqual(jaccard_similarity(text1, text2), 1/6)

    def test_hashtags(self):
        tweet = """RT @HouseGOP: The #StateOfTheUnion isn't strong for the 8.7 million Americans out of work. #SOTU http://t.co/aa7FWRCdyn"""
        self.assertEqual(len(hashtags(tweet)), 2)


if __name__ == '__main__':
    unittest.main()