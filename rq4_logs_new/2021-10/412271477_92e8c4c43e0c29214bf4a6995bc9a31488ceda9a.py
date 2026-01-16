from __future__ import annotations

import atexit
import logging
import logging.handlers
import queue as queue_lib

import time
import typing
# noinspection PyUnresolvedReferences
from logging.config import ConvertingDict, ConvertingList, valid_ident


def _resolve_converting_dict(convert_dict: ConvertingDict) -> typing.Dict:
    # Check for cached conversion result
    if '__resolved__' in convert_dict:
        return convert_dict['__resolved__']

    # Get class and constructor
    init_class = convert_dict.configurator.resolve(convert_dict.pop('class'))

    props = convert_dict.pop('.', None)
    kwargs = {key: convert_dict[key] for key in convert_dict if valid_ident(key)}

    instance = init_class(**kwargs)

    if props:
        for name, value in props.items():
            setattr(instance, name, value)

    # Cache instance
    convert_dict['__resolved__'] = instance

    return instance


def _resolve_converting_list(convert_list: ConvertingList) -> typing.List:
    return [convert_list[i] for i in range(len(convert_list))]


class BufferedHandler(logging.Handler):
    """ Log buffer, useful for presentation of records in a user interface while discarding messages beyond a limit or
    after a specified time.
    """

    def __init__(self, level: int = logging.NOTSET, record_limit: typing.Optional[int] = None,
                 record_timeout: typing.Optional[float] = None):
        """

        :param level:
        :param record_limit:
        :param record_timeout:
        """
        logging.Handler.__init__(self, level)

        self._record_limit = record_limit
        self._record_timeout = record_timeout

        self._record_buffer: typing.List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        # Discard old records
        self._update()

        # Append record to buffer
        self._record_buffer.append(record)

    @property
    def records(self) -> typing.FrozenSet[logging.LogRecord]:
        # Discard old records
        self._update()

        return frozenset(self._record_buffer)

    def _update(self):
        """ Discard records beyond the configured record count limit and/or configures record timeout.
        """
        if self._record_limit:
            while len(self._record_buffer) > self._record_limit:
                self._record_buffer.pop(0)

        if self._record_timeout:
            expiry = time.time() - self._record_timeout

            while len(self._record_buffer) > 0:
                if self._record_buffer[0].created < expiry:
                    self._record_buffer.pop(0)


# Quotes required to ensure compatibility with Python < 3.9
_T_QUEUE = typing.Union['queue_lib.SimpleQueue[typing.Any]', 'queue_lib.Queue[typing.Any]', ConvertingDict]
_T_HANDLERS = typing.Union[typing.Iterable[logging.Handler], ConvertingList]


class QueueListenerHandler(logging.handlers.QueueHandler):
    def __init__(self, handlers: _T_HANDLERS, queue: typing.Optional[_T_QUEUE] = None, queue_size: int = -1,
                 auto_start: bool = True, respect_handler_level: bool = True) -> None:
        if isinstance(queue, ConvertingDict):
            queue = _resolve_converting_dict(queue)
        elif not queue:
            queue = queue_lib.Queue(queue_size)

        if isinstance(handlers, ConvertingList):
            handlers = _resolve_converting_list(handlers)

        logging.handlers.QueueHandler.__init__(self, queue)

        # Create listener thread
        # noinspection PyUnresolvedReferences
        self._listener = logging.handlers.QueueListener(self.queue, *handlers,
                                                        respect_handler_level=respect_handler_level)

        if auto_start:
            self.start()

            # Stop listener on exit
            atexit.register(self.stop)

    def start(self):
        self._listener.start()

    def stop(self):
        self._listener.stop()