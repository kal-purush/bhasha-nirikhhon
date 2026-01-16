import unittest
from unittest.mock import patch, MagicMock
import logging
import os

from logs import Logger


class TestLogin (unittest.TestCase):

    def setUp(self):
        logging.DEBUG = 'DEBUG'
        logging.INFO = 'INFO'
        logging.WARNING = 'WARNING'
        logging.ERROR = 'ERROR'
        logging.CRITICAL = 'CRITICAL'
        self.loggerConfig = {
            'LOG_FORMAT': 'LOG_FORMAT',
            'LOG_DATE_FORMAT': 'LOG_DATE_FORMAT',
            'LOG_LEVEL': 'ERROR'
        }

        os.environ = MagicMock()

    # Test OK: The logger is created to log on a file
    @patch('logging.basicConfig')
    def test_log_file(self, mockBasicConfig):
        runPath = 'run:test:path'
        self.loggerConfig['LOG_FILE'] = 'LOG_FILE'
        self.loggerConfig['LOG_MODE'] = 'LOG_MODE'
        os.environ.__getitem__.side_effect = self.loggerConfig.__getitem__
        os.environ.__iter__.side_effect = self.loggerConfig.__iter__
        os.environ.__contains__.side_effect = self.loggerConfig.__contains__

        logger = Logger(runPath)
        self.assertEqual(runPath, logger.runpath)
        mockBasicConfig.assert_called_with(
            format=os.environ['LOG_FORMAT'],
            datefmt=os.environ['LOG_DATE_FORMAT'],
            level=logging.ERROR,
            filename=os.environ['LOG_FILE'],
            filemode=os.environ['LOG_MODE']
        )

    @patch('logging.basicConfig')
    def test_log_console(self, mockBasicConfig):
        runPath = 'run:test:path'

        os.environ.__getitem__.side_effect = self.loggerConfig.__getitem__
        os.environ.__iter__.side_effect = self.loggerConfig.__iter__
        os.environ.__contains__.side_effect = self.loggerConfig.__contains__

        logger = Logger(runPath)
        self.assertEqual(runPath, logger.runpath)
        mockBasicConfig.assert_called_with(
            format=os.environ['LOG_FORMAT'],
            datefmt=os.environ['LOG_DATE_FORMAT'],
            level=logging.ERROR
        )

    @patch('logging.debug')
    def test_log_debug(self, mockDebug):
        runPath = 'run:test:path'
        message = 'debug message'

        os.environ.__getitem__.side_effect = self.loggerConfig.__getitem__
        os.environ.__iter__.side_effect = self.loggerConfig.__iter__
        os.environ.__contains__.side_effect = self.loggerConfig.__contains__

        logger = Logger(runPath)
        logger.debug(message)

        mockDebug.assert_called_with(f'#{runPath}# {message}')

    @patch('logging.info')
    def test_log_info(self, mockInfo):
        runPath = 'run:test:path'
        message = 'info message'

        os.environ.__getitem__.side_effect = self.loggerConfig.__getitem__
        os.environ.__iter__.side_effect = self.loggerConfig.__iter__
        os.environ.__contains__.side_effect = self.loggerConfig.__contains__

        logger = Logger(runPath)
        logger.info(message)

        mockInfo.assert_called_with(f'#{runPath}# {message}')

    @patch('logging.warning')
    def test_log_warning(self, mockWarning):
        runPath = 'run:test:path'
        message = 'warning message'

        os.environ.__getitem__.side_effect = self.loggerConfig.__getitem__
        os.environ.__iter__.side_effect = self.loggerConfig.__iter__
        os.environ.__contains__.side_effect = self.loggerConfig.__contains__

        logger = Logger(runPath)
        logger.warning(message)

        mockWarning.assert_called_with(f'#{runPath}# {message}')

    @patch('logging.error')
    def test_log_error(self, mockError):
        runPath = 'run:test:path'
        message = 'error message'

        os.environ.__getitem__.side_effect = self.loggerConfig.__getitem__
        os.environ.__iter__.side_effect = self.loggerConfig.__iter__
        os.environ.__contains__.side_effect = self.loggerConfig.__contains__

        logger = Logger(runPath)
        logger.error(message)

        mockError.assert_called_with(f'#{runPath}# {message}')

    @patch('logging.critical')
    def test_log_critial(self, mockCritial):
        runPath = 'run:test:path'
        message = 'Critial message'

        os.environ.__getitem__.side_effect = self.loggerConfig.__getitem__
        os.environ.__iter__.side_effect = self.loggerConfig.__iter__
        os.environ.__contains__.side_effect = self.loggerConfig.__contains__

        logger = Logger(runPath)
        logger.critical(message)

        mockCritial.assert_called_with(f'#{runPath}# {message}')