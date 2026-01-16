# logging_handler_config.py
import logging

_logger = logging.getLogger(__name__)

MESSAGE_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def get_logger():
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler("my_file.log")

    _logger.setLevel(logging.DEBUG)  # main level logging

    # specified level by handler
    stream_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.ERROR)

    stream_handler.setFormatter(logging.Formatter(MESSAGE_FORMAT))
    file_handler.setFormatter(logging.Formatter(MESSAGE_FORMAT))

    _logger.addHandler(stream_handler)
    _logger.addHandler(file_handler)
    return _logger