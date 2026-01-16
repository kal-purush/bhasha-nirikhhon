from setuptools import setup, find_packages

setup(
    name='fringe_pack',
    version='1.0',
    packages=find_packages(),
    package_data={
        'fringe_pack': ['*.pyc']  # Include all .pyc files in the package
    },
    install_requires=[
        'opencv-python',
        'numpy',
        'matplotlib',
    ],
    author='Pavan Mohan Neelamraju',
    author_email='npavanmohan3@gmail.com',
    description='Package for amplitude and phase correction in holograms',
    keywords='image processing fringe_pack',
    url='https://github.com/PavanMohanN/fringe_pack',

)