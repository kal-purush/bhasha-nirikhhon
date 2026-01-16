import os
import io
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with io.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='throwie',
    version='1.0.0',
    description='Throwie - EC2 instances tagging tool',
    long_description=long_description,
    url='https://github.com/b-b3rn4rd/throwie',
    author='Bernard Baltrusaitis',
    author_email='bernard@runawaylover.info',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3'
    ],
    keywords='ec2 tag boto3 cli',
    packages=find_packages(exclude=['docs', 'tests']),
    install_requires=[
        'boto3>=1.4.0',
        'botocore>=1.4.44,<2.0.0'
    ],
    package_data={
        'throwie': ['*.json']
    },
    entry_points={
        'console_scripts': [
            'throwie = throwie.cli:main'
        ]
    }
)