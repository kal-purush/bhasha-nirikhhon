from setuptools import setup, find_packages

def readme():
    with open("README.md") as f:
        return f.read()


setup(
    name='rmq-resilient-libs',
    version='0.1',
    description="Pika consumer which allow for resilient connection, and delay between processing of non-accepted messages",
    long_description=readme(),
    author="Loïs Postula",
    author_email="lois.postula@railnova.eu",
    packages=find_packages(),
    install_requires=[
        "logging",
        "pika"
    ]
)