# -*- coding: utf-8 -*-

from setuptools import setup

readme = ''
history = ''

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='pydst',
    version='0.1.0',
    description="Python Data Structure",
    long_description=readme + '\n\n' + history,
    author="Ivo Lima",
    author_email='ivo.romario@gmail.com',
    url='https://github.com/ivolima/pydst',
    packages=[
        'pydst',
    ],
    package_dir={'pydst': 'pydst'},
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='pydst',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
