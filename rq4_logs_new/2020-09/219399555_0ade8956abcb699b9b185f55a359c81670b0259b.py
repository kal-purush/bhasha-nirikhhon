#!/bin/env python3

""" Unittest for application body """

import unittest
from unittest.mock import Mock, patch

from metafiddler.main import Run

class TestActions(unittest.TestCase):
    """ Test the unix gamepad """

    def test_stop(self):
        """ Test the stop() function """
        self.assertIsNone(Run.stop(Mock()))

if __name__ == "__main__":
    unittest.main()