from typing import List
from collections import namedtuple, defaultdict
from fuzzywuzzy import fuzz, process
from difflib import SequenceMatcher


class FuzzyTextMatcher(object):
    """
    FuzzyTextMatcher is a class for fuzzy text matching.

    Attributes:
        similarity_cutoff (int): Minimum similarity score for a match.
        process_text (callable): Function to preprocess text before matching.
        preserve_order (bool): Flag to indicate if order of matches should be preserved.
        search_bound (tuple): Search boundary for matching.
    """

    def __init__(
        self,
        list_of_strings: List[str],
        similarity_cutoff: int = 80,
        preserve_order: bool = False,
        search_bound: tuple = (-15, +15),
        process_text: callable = None,
    ):
        """
        Initializes FuzzyTextMatcher with the provided parameters.

        Args:
            list_of_strings (List[str]): List of strings to match against.
            similarity_cutoff (int): Minimum similarity score for a match.
            preserve_order (bool): Flag to indicate if order of matches should be preserved.
            search_bound (tuple): Search boundary for matching.
            process_text (callable): Function to preprocess text before matching.
        """
        self.similarity_cutoff = similarity_cutoff
        self.process_text = process_text
        self.preserve_order = preserve_order
        self.search_bound = search_bound
        self._format_texts(list_of_strings)
        self.return_format = namedtuple(
            "MatchedText", ["index", "text", "similarity", "equality"]
        )

    def _format_texts(self, list_of_strings):
        """
        Formats the list of strings for efficient searching.

        Args:
            list_of_strings (List[str]): List of strings to format.
        """
        if self.preserve_order:
            self.text_to_index = defaultdict(list)
            for idx, _string in enumerate(list_of_strings):
                self.text_to_index[_string].append(idx)

        list_of_strings = sorted(list(set(list_of_strings)), key=lambda x: (len(x), x))
        L = 0
        index = {0: 0}
        for idx, string in enumerate(list_of_strings):
            l = len(string)
            if l != L:
                index[l] = idx
                L = l

        index[L + 1] = len(list_of_strings)

        self.texts = list_of_strings
        self.index = index
        self.max_length = len(list_of_strings[-1])

    def __call__(self, text: str, search_bound: tuple = None):
        """
        Performs fuzzy text matching.

        Args:
            text (str): Text to match against.
            search_bound (tuple): Search boundary for matching.

        Returns:
            list: List of matched texts and their scores.
        """
        assert len(text) > 0, "Input text must be greater than zero"
        search_bound = search_bound if search_bound else self.search_bound

        if search_bound:
            l, r = self._get_search_bound(len(text), search_bound)
            texts = self.texts[l:r]
        else:
            texts = self.texts

        if self.preserve_order:
            return self._order_preserving_search(texts, text)
        
        return self._unordered_search(texts, text)

    def _unordered_search(self, texts: str, text: str):
        """
        Performs unordered search for matching.

        Args:
            texts (str): List of texts to search within.
            text (str): Text to match against.

        Returns:
            list: List of matched texts and their scores.
        """
        matched_texts_and_scores = list(
            zip(
                *process.extractWithoutOrder(
                    text,
                    texts,
                    scorer=fuzz.partial_ratio,
                    score_cutoff=self.similarity_cutoff,
                )
            )
        )

        if not matched_texts_and_scores:
            return []

        matched_texts, similarity_scores = matched_texts_and_scores

        sm_scores = [
            self._get_sequence_matcher_score(matched_text, text)
            for matched_text in matched_texts
        ]

        scores = [(s1 + s2 * 2) / 300 for s1, s2 in zip(similarity_scores, sm_scores)]
        equality_scores = [
            self._get_length_equality_score(matched_text, text)
            for matched_text in matched_texts
        ]

        matched_texts = [
            self.return_format(-1, matched_text, round(score, 3), round(equality, 3))
            for matched_text, score, equality in zip(
                matched_texts, scores, equality_scores
            )
        ]

        return sorted(
            matched_texts, key=lambda t: (t.similarity, t.equality), reverse=True
        )

    def _order_preserving_search(self, texts: str, text: str):
        """
        Performs order-preserving search for matching.

        Args:
            texts (str): List of texts to search within.
            text (str): Text to match against.

        Returns:
            dict: Matched texts along with their indices and scores.
        """
        matched_texts_and_scores = list(
            zip(
                *process.extractWithoutOrder(
                    text,
                    texts,
                    scorer=fuzz.partial_ratio,
                    score_cutoff=self.similarity_cutoff,
                )
            )
        )

        if not matched_texts_and_scores:
            return []

        matched_texts, similarity_scores = matched_texts_and_scores

        sm_scores = [
            self._get_sequence_matcher_score(matched_text, text)
            for matched_text in matched_texts
        ]
        scores = [(s1 + s2 * 2) / 300 for s1, s2 in zip(similarity_scores, sm_scores)]

        equality_scores = [
            self._get_length_equality_score(matched_text, text)
            for matched_text in matched_texts
        ]

        matched_text_info = {}
        for matched_text, score, equality in zip(
            matched_texts, scores, equality_scores
        ):
            for idx in self.text_to_index[matched_text]:
                matched_text_info[idx] = self.return_format(
                    idx, matched_text, round(score, 3), round(equality, 3)
                )

        return matched_text_info

    def _get_sequence_matcher_score(self, texts, text):
        """
        Calculates sequence matcher score.

        Args:
            texts (str): Text to compare.
            text (str): Reference text.

        Returns:
            float: Sequence matcher score.
        """
        return SequenceMatcher(None, texts, text).ratio() * 100

    def _get_length_equality_score(self, text_a, text_b):
        """
        Calculates length equality score.

        Args:
            text_a (str): First text.
            text_b (str): Second text.

        Returns:
            float: Length equality score.
        """
        if not text_a or not text_b:
            return 0

        a, b = len(text_a), len(text_b)
        return 1 / max(a / b, b / a)

    def _get_search_bound(self, fuzzy_text_length: int, search_bound: tuple):
        """
        Calculates search boundary.

        Args:
            fuzzy_text_length (int): Length of the text to search.
            search_bound (tuple): Search boundary.

        Returns:
            tuple: Left and right indices of the search boundary.
        """
        l, r = search_bound
        l = max(fuzzy_text_length + l, 0)
        r = min(fuzzy_text_length + r, self.max_length)

        while l not in self.index:
            l = l - 1
        while r not in self.index:
            r = r + 1

        return self.index[l], self.index[r] + 1