# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os


git_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.git')
head_path = os.path.join(git_dir, 'HEAD')
try:
    with open(head_path, 'r') as f:
        head_suffix = f.read().strip().split(' ')[1].replace('/', os.sep)
except IOError:
    head_suffix = os.path.join('refs', 'heads', 'unknown')
branch = head_suffix.split(os.sep)[-1]

path = os.path.join(git_dir, head_suffix)
try:
    with open(path, 'r') as f:
        __version__ = '%s %s' % (f.read().strip()[:7], branch)
except IOError:
    __version__ = 'testing'