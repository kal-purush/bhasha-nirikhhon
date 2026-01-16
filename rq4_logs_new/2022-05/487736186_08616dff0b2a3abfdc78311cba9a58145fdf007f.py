import sys

from numpy.distutils.core import setup, Extension

extra_link_args = []
libraries = []
library_dirs = []

setup(
    name = 'somspherez',
    version = '0.0.1',
    author = 'Hugo Li',
    author_email = 'weqan@outlook.com',
    packages = [],
    py_modules = ['somspherez'],
    description = 'somspherez : Self Organizing Maps in spherical coordinates and other topologies',
    long_description = open('README.md').read(),
    install_requires=['numpy', 'matplotlib', 'scipy'],
)