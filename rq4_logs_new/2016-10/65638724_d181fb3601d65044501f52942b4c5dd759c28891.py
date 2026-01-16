#!/usr/bin/env python
# -*- coding: utf-8 -*-
from trie import Trie


class AutoCompleter(object):
    """Python implementation of autocomplete class."""
    def __init__(self, vocabulary, max_completions=5):
        """Initialize the Autocompleter class."""
        self.vocabulary = Trie(iterable=vocabulary)
        self.max_completions = max_completions

    def __call__(self, token):
        """Takes user input and returns list of suggested words, max five."""
        suggested_words = []
        if not isinstance(token, str):
            return suggested_words
        start = self.vocabulary.root
        completions = 0
        while completions < self.max_completions:
            try:
                start = start[token[completions]]
                for k, v in start.items():
                    if k != '#':
                        print(k)
                        suggested_words.append(start.popitem()[0])
                        completions += 1
                        if len(v) > 0:
                            start = start[token[completions]]
                            suggested_words.append(start.popitem()[0])
            except KeyError:
                return suggested_words

            return suggested_words