# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='sample',
    version='0.0.1',
    description='Skeleton',
    long_description=readme,
    author='Karoly Albert Szabo',
    author_email='szabo.karoly.a@gmail.com',
    url='https://github.com/sabau/kaggle',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)