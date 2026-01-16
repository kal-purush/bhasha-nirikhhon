from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()

setup(
    name='flickr-tools',
    version='0.1',
    description='Tools for uploading to and downloading from Flickr.',
    long_description=readme(),
    url='https://github.com/simeon-walker/flickr-tools.git',
    author='Simeon Walker',
    author_email='',
    license='GPL',
    packages=['flickr_tools'],
    install_requires=[
      'flickr_api',
    ],
)