import unittest
import contextlib
from StringIO import StringIO

import easyxml
from easyxml import elements, dump, dumps


@contextlib.contextmanager
def strio(buffer=''):
    sio = StringIO(buffer)
    try:
        yield sio
    finally:
        sio.close()


class ConverterTester(unittest.TestCase):

    def test_basic_case(self):
        self.assertEqual('<root />', dumps(['root']))

    def test_basic_print_case(self):
        with strio() as f:
            dump(['root'], f)
            self.assertEqual('<root />\n', f.getvalue())