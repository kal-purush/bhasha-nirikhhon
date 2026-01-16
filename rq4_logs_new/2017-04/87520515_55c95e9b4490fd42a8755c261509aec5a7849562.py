import sys
from setuptools import setup, find_packages

sys.path.append('./pir')
sys.path.append('./test')

setup(
    name='pir',
    version='0.0',
    packages=find_packages(),
    test_suite='test',
    package_data={
        '': ['*.yaml']
    },
    install_requires=[
        "boto",
        "google-api-python-client",
        "awscli",
        "PyYAML",
        "numpy",
        "scipy",
        "bottle",
        "gunicorn",
        "pandas",
        "redis",
        "scikit-learn",
        "psycopg2",
        "fluent-logger",
        "PyMySQL",
        "pystan",
        "bottle",
        "sqlalchemy",
        "jupyter",
        "jinja2",
        "beautifulsoup4",
        "ipython-sql",
        "gensim"
    ]
)