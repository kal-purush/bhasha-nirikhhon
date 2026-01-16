# coding: utf-8

# pylint: disable=missing-docstring
# pylint: disable=invalid-name

import os
import glob

from setuptools import setup, find_packages

import os
import glob

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, '__version__'), encoding='utf-8') as f:
    __version__ = f.read().strip()

with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    __readme__ = f.read().strip()

__app_name__ = 'pyjamampeople'

def main():
    setup(
        name=__app_name__,
        version=__version__,
        description='example using asyncio, tornado and websocket together',
        long_description_content_type='text/x-rst',
        long_description=__readme__,
        url='',
        author='giovanni angeli',
        author_email='giovanni.angeli6@gmail.com',
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Programming Language :: Python :: 3',
            "License :: OSI Approved :: MIT License",
            "Operating System :: POSIX :: Linux",
        ],
        packages=find_packages(where='src'),
        package_dir={'': 'src'},
        data_files=[
            ('templates', list(glob.glob('templates/*'))),
        ],
        include_package_data=True,
        scripts=[
            'bin/pyjamampeople',
        ],
        install_requires=[
            'tornado',
        ],
    )


if __name__ == '__main__':
    main()