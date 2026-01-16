import unittest
from mock import Mock
import sys
import src.gui_config as gui_config
import src.argument_parser as argument_parser




class TestLoadsDefault(unittest.TestCase):
    sys.argv[1:] = ['--config', 'default_config.yml']
    args = argument_parser.parse_args()
    config = gui_config.Configuration(args)

    def test_fsize(self):
        self.assertEqual(self.config.font_size, 15)


if __name__ == '__main__':
    unittest.main()