from setuptools import setup, find_packages, Extension, Distribution
import glob, os
from codecs import open
from os import path

class BinaryDistribution(Distribution):
    def has_ext_modules(foo):
        return True

library_files = []

for file in glob.glob("*.so"):
    library_files.append(('lib', [str(file)]))

for file in glob.glob("*.so.*"):
    library_files.append(('lib', [str(file)]))

setup(  name='pplpy_dependencies', 
        version='0.0.1',
        description='precompiled shared libraries for pplpy',
        data_files=library_files
)
