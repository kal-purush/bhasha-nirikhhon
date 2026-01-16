#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

from setuptools import setup, find_packages


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    init_py = open(os.path.join(package, '__init__.py')).read()
    return re.match("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


version = get_version('superutils')

LONG_DESCRIPTION = open('README.rst').read()

setup(
    name="superutils",
    version=version,
    zip_safe=True,
    description="Superutils package",
    long_description=LONG_DESCRIPTION,
    classifiers=[
        "Environment :: Python Environment",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Private :: Do Not Upload",
    ],
    keywords='superutils',
    packages=find_packages(),
    include_package_data=True
)