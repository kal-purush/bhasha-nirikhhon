from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()

long_description = 'Coming soon...'

setup(
    name='pat-match',
    version='1.0.4',
    author='Balraj Singh Sainih',
    author_email='balraj0496@gmail.com',
    url='https://github.com/balrajsingh9/esa-utils',
    description='algorithm implementations based on enhanced suffix arrays',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=requirements,
    zip_safe=False
)