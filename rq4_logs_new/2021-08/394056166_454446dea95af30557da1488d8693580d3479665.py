#!/usr/bin/env python
# $Id: dfae04b930fada61e023447f283492612bf42111 $

from setuptools import setup, find_packages

from weaves import __version__

setup(
    name='weaves',
    version=__version__,
    setup_requires=['pbr>=1.9', 'setuptools>=17.1'],
    install_requires=[
          'markdown',
          'unidecode',
          'cached-property',
    ],
    pbr=True,
)