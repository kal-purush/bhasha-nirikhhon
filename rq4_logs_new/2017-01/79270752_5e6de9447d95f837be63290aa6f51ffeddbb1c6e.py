from math import log
from collections import Iterable
from collections import Counter
import itertools

import numpy as np

e = np.exp(1.)

class _BaseNGram(object):
    """Base class for NGram models

    Parameters
    ----------
    special_token : bool, optional, default False
        If True, special token `<S>` and `</S>` will be appended to the 
        begining and end of document."""
    
    def __init__(self, special_token=False):
        if not isinstance(special_token, bool):
            raise TypeError("special token must be bool.")
        self._special_token = special_token

    def _get_mle_count(self, corpus, n=1):
        """Compute the Maximum-likelihodd estimate of ngram count 

        Parameters
        ----------
        corpus : Iterable
            Training corpus. Element of the corpus corresponds to document (
            Iterable of str)

        n : int, optional, default 1
            Size of the token sequence. For example, n = 1 for unigram

        Returns
        -------
        mle_count : dict
            Contains the unique ngrams as keys, counts as values."""
        if not isinstance(n, int) or n < 1:
            raise TypeError("n must be integer >= 1.")

        mle_count = Counter()
        for tokens in corpus:
            if not isinstance(tokens, Iterable):
                raise TypeError("element of corpus must be Iterable: %s")
            mle_count.update(self._get_ngram_iter(tokens, n=n))
        mle_count = dict(mle_count)
        if len(mle_count) == 0:
            raise ValueError("corpus must be non-empty.")

        return mle_count

    def _get_ngram_iter(self, tokens, n=1):
        """Convert sequence to tokens into ngrams

        Example: ["a", "b", "c"] => [("a", "b"), ("b", "c")] for bigram

        Parameters
        ----------
        tokens : Iterable
            A document represented as sequence of str
        
        n : int, optional, default 1
            Size of the token sequence. For example, n = 1 for unigram

        Returns
        -------
        Generator returns one ngram at a time. Each ngram is represented as a
        tuple of str."""

        ngram = []
        if self._special_token:
            if n == 1:
                yield ("<s>",)
            else:
                ngram = ["<s>"] * (n - 1)
        for token in tokens:
            if not isinstance(token, basestring) or len(token) == 0:
                raise TypeError("token must be non-empty str.")
            ngram.append(token)
            if len(ngram) == n:
                yield tuple(ngram)
                ngram.pop(0)
        if self._special_token:
            ngram.append("</s>")
            yield tuple(ngram)

    def _validate_fit_input(self, corpus, n0):
        """Validate input of fit function"""

        if not isinstance(corpus, Iterable):
            raise TypeError("corpus must be Iterable.")
        if not isinstance(n0, int) or n0 < 0:
            raise TypeError("out-of-vocabulary size must be an integer >= 0.")

    def _validate_predict_input(self, tokens, normalize, base):
        """Validate input of predict function"""

        if not isinstance(tokens, Iterable):
            raise TypeError("tokens must be Iterable.")
        if not isinstance(normalize, bool):
            raise TypeError("normalize must be bool.")
        if not isinstance(base, float) or base < 0:
            raise TypeError("base must be float >= 0.")
        
class Unigram(_BaseNGram):
    """Unigram model"""

    def fit(self, corpus, n0=0):
        """Fit Unigram model from training corpus

        Parameters
        ----------
        corpus : Iterable
            Training corpus. Element of the corpus corresponds to document (
            Iterable of str)

        n0 : int, optional, default 0
            The number of unique out-of-vocabulary tokens (i.e. N0)"""

        self._validate_fit_input(corpus, n0)
        mle_count = self._get_mle_count(corpus, n=1)

        corpus_length = float(sum(mle_count.values()))
        n_mle_count = dict(Counter(mle_count.values()))
        gt_count = dict()
        n_mle_count[0] = n0
        counts = sorted(n_mle_count.keys())
        for r in range(len(counts) - 1):
            gt_count[counts[r]] = counts[r] if n0 == 0 else \
                (counts[r + 1] * n_mle_count[counts[r + 1]]) / \
                float(n_mle_count[counts[r]])
        gt_count[counts[-1]] = counts[-1]

        gt_prob = {k: (v / corpus_length) for k, v in gt_count.iteritems()}

        prob_mass = reduce(lambda x,y: x+y, \
            map(lambda r: n_mle_count[r] * gt_prob[r], gt_prob.keys()), 0.0)
        gt_prob = {k: v / prob_mass for k, v in gt_prob.iteritems()}

        self.gt_prob_ = gt_prob
        self.mle_count_ = mle_count
        self.n_mle_count_ = n_mle_count

    def predict(self, tokens, normalize=True, base=e):
        """Predict the log-transformed probability of a list of tokens
        
        Parameters
        ----------
        tokens : Iterable
            Sequence of tokens to be evaluated

        normalize : bool, optional, default True
            If True, divide the sum of log-probabilities by the length of token
                
        base : float, optional, default 2.
            Base of logarithm

        Returns
        -------
        score : float
            Probability score of input sequence of tokens"""
       
        self._validate_predict_input(tokens, normalize, base)
        unigram_list = [unigram for unigram in self._get_ngram_iter(tokens)]

        if len(unigram_list) == 0:
            raise ValueError("tokens must be non-empty.")
        
        scores = [self.logprob(u, base) for u in unigram_list]
        score = reduce(lambda x,y: x+y, scores, 0.0)
        if normalize:
            score = score / float(len(unigram_list))
        return score

    def logprob(self, ngram, base=e):
        """Compute log-transformed probability of unigram

        Parameters
        ----------
        ngram : tuple
            Input ngram (tuple of str)

        base : float, optional, default 2.
            Base of logarithm

        Returns
        -------
        score : float
            Probaility score of input unigram"""

        mle_count = self.mle_count_
        gt_prob = self.gt_prob_

        if ngram in mle_count:
            score = log(gt_prob[mle_count[ngram]], base)
        else:
            score = log(gt_prob[0], base)

        return score               

class Bigram(_BaseNGram):
    """Bigram language model"""

    def fit(self, corpus, n0=0):
        """Fit Bigram model from training corpus

        Parameters
        ----------
        corpus : Iterable
            Training corpus. Element of the corpus corresponds to document (
            Iterable of str)

        n0 : int, optional, default 0
            The number of unique out-of-vocabulary tokens (i.e. N0)"""

        self._validate_fit_input(corpus, n0)
        corpus_iter_bi, corpus_iter_un = itertools.tee(corpus)

        mle_count = self._get_mle_count(corpus_iter_bi, n=2)

        unigram = Unigram(self._special_token)
        unigram.fit(corpus_iter_un, n0)

        self.mle_count_ = mle_count
        self.unigram_ = unigram

    def predict(self, tokens, alpha=0.4, normalize=True, base=e):
        """Predict the log-transformed probability of a list of tokens
        
        Parameters
        ----------
        tokens : Iterable
            Sequence of tokens to be evaluated

        alpha : float
            Parameter of the Stupid backoff method

        normalize : bool, optional, default True
            If True, divide the sum of log-probabilities by the length of token
                
        base : float, optional, default 2.
            Base of logarithm

        Returns
        -------
        score : float
            Probability score of input sequence of tokens"""

        self._validate_predict_input(tokens, normalize, base)
        if not isinstance(alpha, float) or alpha < 0. or alpha > 1.:
            raise TypeError("alpha must be float >= 0 and <= 1.")

        bigram_list = [bigram for bigram in self._get_ngram_iter(tokens, 2)]
        if len(bigram_list) == 0:
            raise ValueError("tokens must be non-empty.")

        bigram_mle_count = self.mle_count_
        unigram_mle_count = self.unigram_.mle_count_
        unigram = self.unigram_

        scores = [log(float(bigram_mle_count[x]) / unigram_mle_count[x[:1]],
            base)
            if x in bigram_mle_count
            else (log(alpha, base) + unigram.logprob(x[-1:], base))
            for x in bigram_list]
        
        score = reduce(lambda a,b: a+b, scores, 0.0)

        if normalize:
            score = score / float(len(bigram_list))
        return score