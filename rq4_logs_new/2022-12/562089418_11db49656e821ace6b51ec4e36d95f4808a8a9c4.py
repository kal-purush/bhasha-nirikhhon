from setuptools import setup, find_packages

setup(
    name="colorizer",
    py_modules=["colorizer"],
    packages=find_packages(exclude=["tests*"]),
    install_requires=["opencv_transforms"],
)