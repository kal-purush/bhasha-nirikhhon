from distutils.core import setup

with open('requirements.txt') as f:
    requirements = [l.strip() for l in f]

setup(
    name='wikibot',
    version='0.1',
    packages=['wikibot'],
    author='Elijah Caine',
    author_email='elijahcainemv@gmail.com',
    url='https://github.com/elijahcaine/wikibot',
    install_requires=requirements,
    package_data={'cah': ['requirements.txt', 'README.md', 'LICENSE']}
)