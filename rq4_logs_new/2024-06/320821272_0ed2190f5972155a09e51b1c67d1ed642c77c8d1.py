from numba import njit
from scipy.sparse import coo_matrix
import numpy as np


@njit(cache=True)
def number_of_factors(length, n_range=0):
    """
    Return the number of factors (with multiplicity) of size at most `n_range` that exist in a text of length `length`.
    This allows to pre-allocate working memory.

    Parameters
    ----------
    length: :py:class:`int`
        Length of the text.
    n_range: :py:class:`int`, optional
        Maximal factor size. If 0, all factors are considered.

    Returns
    -------
    int
        The number of factors (with multiplicity).

    Examples
    --------
    >>> l = len("riri")
    >>> number_of_factors(l)
    10
    >>> number_of_factors(l, n_range=2)
    7
    """
    if n_range == 0  or n_range > length:
        return length * (length + 1) // 2
    return n_range * (length - n_range) + n_range * (n_range + 1) // 2


@njit(cache=True)
def default_preprocessor(txt):
    """
    Default string preprocessor: trim extra spaces and lower case from string `txt`.

    Parameters
    ----------
    txt: :py:class:`str`
        Text to process.

    Returns
    -------
    :py:class:`str`
        Processed text.

    Examples
    ---------
    >>> default_preprocessor(" LaTeX RuleZ    ")
    'latex rulez'
    """
    return txt.strip().lower()


@njit(cache=True)
def empty_features():
    return {'a': 1 for _ in range(0)}


@njit(cache=True)
def jit_fit_transform(corpus, preprocessor, features, n_range):
    ptr=0
    m = len(features)
    tot_size = 0
    for txt in corpus:
        tot_size += number_of_factors(len(preprocessor(txt)), n_range)
    feature_indices = np.zeros(tot_size, dtype=np.uint)
    document_indices = np.zeros(tot_size, dtype=np.uint)
    for i, txt in enumerate(corpus):
        start_ptr = ptr
        txt = preprocessor(txt)
        length = len(txt)
        for start in range(length):
            end = min(start+n_range, length) if n_range>0 else length
            sub_text = txt[start:end]
            for current in range(1, end-start+1):
                factor = sub_text[:current]
                if factor in features:
                    j = features[factor]
                else:
                    features[factor] = m
                    j = m
                    m += 1
                feature_indices[ptr] = j
                ptr += 1
        document_indices[start_ptr:ptr] = i
    return tot_size, document_indices, feature_indices, m


@njit(cache=True)
def jit_fit(corpus, preprocessor, features, n_range):
    m=len(features)
    for txt in corpus:
        txt = preprocessor(txt)
        length = len(txt)
        for start in range(length):
            end = min(start+n_range, length) if n_range>0 else length
            sub_text = txt[start:end]
            for current in range(1, end-start+1):
                factor = sub_text[:current]
                if factor not in features:
                    features[factor] = m
                    m += 1


@njit(cache=True)
def jit_sampling_fit(corpus, preprocessor, features, n_range, rate, seed=None):
    if seed is not None:
        np.random.seed(seed)
    m=len(features)
    for txt in corpus:
        txt = preprocessor(txt)
        length = len(txt)
        for start in range(length):
            if np.random.rand() < rate:
                end = min(start+n_range, length) if n_range>0 else length
                sub_text = txt[start:end]
                for current in range(1, end-start+1):
                    factor = sub_text[:current]
                    if factor not in features:
                        features[factor] = m
                        m += 1


@njit(cache=True)
def jit_transform(corpus, preprocessor, features, n_range):
    ptr = 0
    m = len(features)
    tot_size = 0
    for txt in corpus:
        tot_size += number_of_factors(len(preprocessor(txt)), n_range)
    feature_indices = np.zeros(tot_size, dtype=np.uint)
    document_indices = np.zeros(tot_size, dtype=np.uint)
    for i, txt in enumerate(corpus):
        start_ptr = ptr
        txt = preprocessor(txt)
        length = len(txt)
        for start in range(length):
            end = min(start + n_range, length) if n_range > 0 else length
            sub_text = txt[start:end]
            for current in range(1, end - start + 1):
                factor = sub_text[:current]
                if factor in features:
                    j = features[factor]
                    feature_indices[ptr] = j
                    ptr += 1
        document_indices[start_ptr:ptr] = i

    feature_indices = feature_indices[:ptr]
    document_indices = document_indices[:ptr]

    return ptr, document_indices, feature_indices, m