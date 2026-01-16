from setuptools import setup
from setuptools.extension import Extension

extensions = Extension('note', 
                sources=['libs/note.cpp'],
                include_dirs=['/opt/homebrew/include'],
                library_dirs=['/opt/homebrew/Cellar/boost-python3/1.79.0_1/lib'],
                runtime_library_dirs=['/opt/homebrew/Cellar/boost-python3/1.79.0_1/lib'],
                libraries=['boost_python3'])

setup(ext_modules=[extensions])