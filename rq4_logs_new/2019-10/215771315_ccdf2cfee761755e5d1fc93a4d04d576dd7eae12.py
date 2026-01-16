# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name="the_final_countdown",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    zip_safe=False,
    entry_points={"console_scripts": ["the_final_countdown=the_final_countdown.main:main"]},
)