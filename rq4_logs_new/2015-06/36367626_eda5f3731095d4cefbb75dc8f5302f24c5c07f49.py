#!/usr/bin/env python

from distutils.core import setup

setup(
    name = 'gogplot',
    version = '0.0.1',
    author = 'Leon du Toit',
    author_email = 'dutoit.leon@gmail.com',
    packages = ['gogplot'],
    url = 'https://github.com/leondutoit/unblog',
    license = 'LICENSE',
    description = 'unblog your blog',
    long_description = open('README.md').read(),
    install_requires = [
        "pytumblr = 0.0.6"
    ],
)