from distutils.core import setup

from securenative.config import sdk_version

setup(
    name='securenative-sdk',
    packages=['securenative-sdk'],
    version=sdk_version,
    license='MIT',
    description='Secure Native SDK for python',
    author='Secure Native',
    author_email='support@securenative.com',
    url='http://www.securenative.com',
    download_url='https://github.com/user/reponame/archive/v_01.tar.gz',  # I explain this later on
    keywords=["securenative", 'cyber-security'],
    install_requires=[
        "requests",
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)