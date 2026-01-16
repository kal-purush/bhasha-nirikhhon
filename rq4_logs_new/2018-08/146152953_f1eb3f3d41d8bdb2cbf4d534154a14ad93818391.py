#!/usr/bin/env python

from __future__ import division

import sys

####################################
# Python2 compatibility for urllib #
####################################

try:
    import urllib
    import urllib2
    import urlparse

    class dummy_urllib_module:
        parse = urlparse
        request = urllib2
    sys.modules["urllib"] = dummy_urllib_module
    sys.modules["urllib.parse"] = dummy_urllib_module.parse
    sys.modules["urllib.request"] = dummy_urllib_module.request
    dummy_urllib_module.parse.urlencode = urllib.urlencode

except ImportError:
    pass


##################
# Useful imports #
##################

import base64
import collections
import datetime
import hashlib
import io
import json
import math
import os
import pickle
import random
import re
import socket
import struct
import subprocess
import tempfile
import time
import urllib
import urllib.parse
import urllib.request


#######################
# Readline completion #
#######################

def setup_readline_completion():
    import atexit
    import readline
    import rlcompleter

    history_path = os.path.expanduser("~/.python_history")

    default_completer = rlcompleter.Completer()

    readline.parse_and_bind("tab: complete")

    if os.path.exists(history_path):
        readline.read_history_file(history_path)

    def my_completer(text, state):
        if text.strip() == "":
            if state == 0:
                return text + "\t"
            else:
                return None
        else:
            return default_completer.complete(text, state)
    readline.set_completer(my_completer)

    def save_history(history_path=history_path):
        readline.write_history_file(history_path)
    atexit.register(save_history)

setup_readline_completion()