from setuptools import setup, find_packages

setup(
    name="gitf",  # Name of your package
    version="1.0.0",  # Package version
    description="A utility program to use to modify a single file from you github repository",  # Short description
    author="John Delvin",  # Author name,
    author_email="johndelvin51@gmail.com",  # Author email
    url="https://github.com/john4650-hub/gitf",  # URL to your project (e.g., GitHub repo)
    packages=find_packages(where="src"),  # Automatically find and include all packages
    package_dir={"": "src"},
    entry_points={
        'console_scripts': [
            'gitf = main:main',
        ]},
    install_requires=[  # List of dependencies your project needs
        "requests"
    ],
    classifiers=[  # Metadata for PyPI
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: linux",
    ],
    python_requires=">=3.5",  # Minimum Python version required
)