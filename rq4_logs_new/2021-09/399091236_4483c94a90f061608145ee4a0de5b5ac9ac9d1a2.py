"""
__init__
~~~~~~~~

Initialization for the imgfilt module.
"""
from imgfilt.imgfilt import *
from imgfilt import imgfilt
from imgfilt.utility import get_prefixed_functions


# Create a dictionary to allow easier discovery and validation of
# the filters available in the module.
filters = get_prefixed_functions('filter_', imgfilt)