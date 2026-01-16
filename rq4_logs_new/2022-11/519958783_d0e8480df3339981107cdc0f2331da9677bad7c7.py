from setuptools import setup

REQUIREMENTS = [
    'nltk',
    'pandas',
    'tqdm',
    'psutil',
    'sklearn'
]

setup(
    name='nb_space_restorer',
    version='0.1',
    description='Train Naive Bayes-based statistical machine learning models for restoring spaces to unsegmented sequences of input characters',
    author='Laurence Dyer',
    author_email='ljdyer@gmail.com',
    url='https://github.com/ljdyer/Naive-Bayes-Space-Restorer',
    packages=['nb_space_restorer'],
    install_requires=REQUIREMENTS
)