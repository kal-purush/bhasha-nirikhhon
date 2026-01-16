# This code is part of Qiskit.
#
# (C) Copyright IBM 2018, 2021.
#
# This code is licensed under the Apache License, Version 2.0. You may
# obtain a copy of this license in the LICENSE.txt file in the root directory
# of this source tree or at http://www.apache.org/licenses/LICENSE-2.0.
#
# Any modifications or derivative works of this code must retain this
# copyright notice, and modified files need to carry a notice indicating
# that they have been altered from the originals.

import setuptools
import os

long_description = """Library for runtime demo."""

with open('requirements.txt', 'r') as f:
    install_requirements = [
        s for s in [
            line.split('#', 1)[0].strip(' \t\n') for line in f
        ] if s != ''
    ]


VERSION_PATH = os.path.join(os.path.dirname(__file__), "VERSION.txt")
with open(VERSION_PATH, "r") as version_file:
    VERSION = version_file.read().strip()

setuptools.setup(
    name='qiskit-runtime-demo',
    version=VERSION,
    description='Qiskit runtim',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/iuliazidaru/qiskit-runtime-demo',
    author='Iulia Zidaru',
    author_email='-',
    license='',
    classifiers=(
        "Environment :: Console",
        "License :: ",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering"
    ),
    keywords='',
    packages=setuptools.find_namespace_packages(include=['qiskit_runtime_demo.*']),
    install_requires=install_requirements,
    include_package_data=True,
    python_requires=">=3.7",
    zip_safe=False
)