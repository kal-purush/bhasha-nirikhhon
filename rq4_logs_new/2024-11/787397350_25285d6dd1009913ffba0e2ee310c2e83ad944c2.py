from setuptools import setup, find_packages

setup(
    name='tpack',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        "numpy",
        "opencv-python",
        "matplotlib",
        "Pillow",
        "torchmetrics"
    ],
    author='Amirtaha',
    author_email='aghasiamirtaha@gmail.com',
    description='TPackage for Video and Image Processing, Plotting and AI',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Amirt55/TPackage.git',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
)