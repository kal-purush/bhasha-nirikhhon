# -*- coding=utf-8 -*-

import itertools
import sys

__all__ = ("range", "map", "filter", "filterfalse", "zip", "zip_longest")

if sys.version_info < (3,):
    # Python 2.7
    range = xrange
    map = itertools.imap
    filter = itertools.ifilter
    filterfalse = itertools.ifilterfalse
    zip = itertools.izip
    zip_longest = itertools.izip_longest
else:
    # Python 3
    range = range
    map = map
    filter = filter
    filterfalse = itertools.filterfalse
    zip = zip
    zip_longest = itertools.zip_longest