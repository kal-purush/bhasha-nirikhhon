from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='rpi_devices',
    version='0.1.0',
    description='A library for operating devices connected to a Raspberry Pi',
    long_description=long_description,
    author='Munir Contractor',
    author_email='munircontractor@gmail.com',
    url='https://github.com/munircontractor/raspberry-pi-device-library',
    keywords=['raspberrypi', 'GPIO'],
    packages=find_packages(exclude=['docs'])
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
    ],
    license='LGPL',
    install_requires=['RPi.GPIO'],
)