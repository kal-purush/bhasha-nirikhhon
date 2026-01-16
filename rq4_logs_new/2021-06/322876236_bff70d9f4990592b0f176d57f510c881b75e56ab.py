#!/usr/bin/env python
#-*- coding:utf-8 -*-

from __future__ import division, print_function

import io
import os
import re

from setuptools import setup         


long_description = """
============
w600tool.py
============

A Python-based firmware upload tool for Winner Micro W600 & W601 WiFi Chips.

Usage
--------

Please see the `Usage section of the README.md file <https://github.com/wemos/w600tool#usage>`_.

You can also get help information by running `w600tool.py --help`.

"""


# http://python-packaging-user-guide.readthedocs.org/en/latest/single_source_version/
def read(*names, **kwargs):
    with io.open(
            os.path.join(os.path.dirname(__file__), *names),
            encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

if os.name == "nt":
    scripts = None
    entry_points = {
        'console_scripts': [
            'w600tool.py=w600tool:_main',

        ],
    }
else:
    scripts = ['w600tool.py',
    ]
    entry_points = None

setup(
    name = "w600tool",
    py_modules=['w600tool'],      
    version = find_version('w600tool.py'),  
    keywords = ("w600","w600tool"),
    description = "A Firmware upload tool for Winner Micro W600 & W601 WiFim",
    long_description = long_description,

    url = "https://github.com/wemos/w600tool",    
    author = "wemos",
    author_email = "support@wemos.cc",

    include_package_data = True,
    platforms = "any",
    install_requires = [
        "pyserial>=3.0",
        "argparse",
        "pyprind",
        "xmodem",
    ],

    scripts=scripts,
    entry_points=entry_points,          
)
