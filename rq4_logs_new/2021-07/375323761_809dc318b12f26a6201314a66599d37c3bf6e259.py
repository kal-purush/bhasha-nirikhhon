import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="adv_train",
    version="0.1",
    author="Hugo Berard, Chiara Regniez",
    author_email="berard.hugo@gmail.com",
    description="Adversarial Training with Langevin Dynamics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ChiaraRgnz/Adversarial-Training",
    project_urls={
        "Bug Tracker": "https://github.com/ChiaraRgnz/Adversarial-Training/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    install_requires=[
        'torch>=1.0.0',
        'advertorch @ git+https://github.com/hugobb/advertorch#egg=advertorch',
        'tqdm',
        'omegaconf',
        'torchvision',
      ],
)