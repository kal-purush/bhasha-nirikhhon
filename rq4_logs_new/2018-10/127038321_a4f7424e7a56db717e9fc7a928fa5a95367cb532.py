from setuptools import setup

setup(
    name="tiptabs",
    version="1.0.0",
    packages=["Tiptabs"],
    install_requires=[
        "Flask==0.12.2",
        "Flask-API==0.7.1",
        "Jinja2==2.9.6",
        "requests==2.19.1",
        "MySQL-connector-python==8.0.12",
    ],
    url="https://github.com/mcdonagj/Tiptabs",
    license="MIT",
    author="Gary McDonald",
    entry_points={
        'console_scripts': ['tiptabs = Tiptabs.main:main'],
    },
)