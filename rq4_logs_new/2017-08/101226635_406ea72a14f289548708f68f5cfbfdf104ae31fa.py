# Although my project is not yet properly packaged to run as executable, But below is an example of
# How would my setup.py file look like

from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='TopicEngine',
    version='1.0.0',

    description='Project assignment for BrightEdge Interview',


    # Author details
    author='Tirthkumar Parikh',
    author_email='tirthmparikh@gmail.com',

    # Choose your license
    license='',


    packages=find_packages('topic_engine', exclude=['docs', 'tests']),


    entry_points={


    },
)