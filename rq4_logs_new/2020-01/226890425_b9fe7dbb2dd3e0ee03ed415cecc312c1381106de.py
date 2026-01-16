
from setuptools import setup, find_packages
from toa_simu import __version__, __package__

setup(
    name=__package__,
    version=__version__,
    packages=find_packages(exclude=['build']),
    package_data={ '': ['data/*']
     },
    include_package_data=True,

    url='',
    license='MIT',
    author='T. Harmel',
    author_email='tristan.harmel@gmail.com',
    description='tools to simulate and visualize radiance spectra for water-atmosphere systems',

    # Dependent packages (distributions)
    install_requires=['pandas','numpy','netCDF4',
                      'matplotlib','docopt'],

    entry_points={
          'console_scripts': [
              #'RTxploitation = RTxploitation.visu:main'
          ]}
)