#
# This file is part of BEAVr, https://github.com/theoryinpractice/beavr/, and is
# Copyright (C) North Carolina State University, 2016. It is licensed under
# the three-clause BSD license; see LICENSE.
#

from setuptools import setup

# Convert README from Markdown to reStructuredText.  This requires the pypandoc
# module; if this module is not present the long_description will be empty so
# make sure you have it when creating packages for PyPI.
try:
    import pypandoc
    long_description = pypandoc.convert("README.md", "rst")
except (IOError, ImportError):
    long_description = ""

setup(
    name = "BEAVr",
    version = "1.0",
    author = "Yang Ho, Clayton G. Hobbs, Brandon Mork, Felix Reidl, "
        "Nishant Rodrigues, Blair Sullivan",
    author_email = "blair_sullivan@ncsu.edu",
    description = "Bounded Expansion Algorithm Visualizer",
    license = "BSD",
    keywords = "concuss graph network visualization",
    url = "https://www.github.com/theoryinpractice/BEAVr",
    packages = ["beavr", "beavr.concuss"],
    long_description = long_description,
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Environment :: MacOS X",
        "Environment :: X11 Applications",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Programming Language :: Python :: 2 :: Only",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering :: Visualization"
    ],
    install_requires=["matplotlib", "networkx", "numpy", "pydot", "wxPython"],
    package_data = {
        "beavr": ["data/icons/*", "data/palettes/*"]
    },
    entry_points = {
        "console_scripts": [
            "beavr=beavr.visapplication:run"
        ]
    }
)