import os

from setuptools import find_packages
from setuptools import setup

with open(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md"), encoding="utf-8",
) as f:
    long_description = f.read()

setup(
    name="predictit",
    version="0.0.1",
    author="Daniel Suo",
    author_email="danielsuo@gmail.com",
    description="mcmcmcmcmc",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/danielsuo/photo",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=["monte carlo", "election"],
    python_requires=">=3.8",
    install_requires=[
        "numpy",
        "matplotlib",
        "jupyter",
        "ipython",
        "requests"
    ],
)