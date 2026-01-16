"""Little helpers."""

from logging import DEBUG, INFO, FileHandler, Formatter, StreamHandler, getLogger
from os import environ
from sys import stdout

from elasticsearch import Elasticsearch


def init_logger(logger=None, lvl=None):
    """Initialize the given logger.
    
    If no logger is passed into the function, the root logger will be retrieved."""
    if logger is None:
        logger = getLogger()
    if isinstance(logger, str):
        logger = getLogger(logger)
    if lvl is None:
        lvl = int(get_setting("LOG_LEVEL", "30"))

    fmt = Formatter("%(asctime)-15s %(threadName)s %(message)s")

    stdout_handler = StreamHandler(stdout)
    stdout_handler.setLevel(DEBUG)
    stdout_handler.setFormatter(fmt)
    logger.addHandler(stdout_handler)

    output_file = get_setting("LOG_OUTPUT")
    if output_file is not None:
        file_handler = FileHandler(output_file)
        file_handler.setLevel(DEBUG)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    logger.setLevel(lvl)

    return logger


def get_setting(key, default=None):
    """Return the env setting corresponding to the given key.

    If key is not found, return the default value.
    """
    return environ.get(key) or default


def app_settings():
    """Retrieve app settings from environment."""
    return dict(
        CACHE_DEFAULT_TIMEOUT=int(get_setting("CACHE_DEFAULT_TIMEOUT", "600")),
        CACHE_TYPE=get_setting("CACHE_TYPE", "null"),
        SECRET_KEY=get_setting("FLASK_SECRET_KEY"),
        TESTING=(get_setting("FLASK_TESTING", "False").capitalize() == "True"),
    )


def elastic(ssl=False):
    """Return an initialized Elastic client."""
    protocol = "https" if ssl else "http"
    host = get_setting("ELASTIC_HOST", "localhost")
    port = get_setting("ELASTIC_PORT", "9200")
    return Elasticsearch(f"{protocol}://{host}:{port}")