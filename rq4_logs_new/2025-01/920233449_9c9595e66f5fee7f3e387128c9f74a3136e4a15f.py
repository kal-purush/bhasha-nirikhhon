from setuptools import setup, find_packages

setup(
         name="security-analyzer",
        version="1.0.0",
        packages=find_packages(where="src"),
        package_dir={"": "src"},
        install_requires=[
        # Укажите зависимости здесь
        ],
         entry_points={
        'console_scripts': [
            'security-analyzer=main:main',
        ],
     },
    )