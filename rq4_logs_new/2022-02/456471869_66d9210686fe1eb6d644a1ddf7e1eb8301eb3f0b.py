import sys
import os
import unittest

sys.path.append(os.path.join(os.getcwd(), '..'))

from client import create_presence, process_ans
from common.variables import ACTION, ACCOUNT_NAME, RESPONSE, PRESENCE, TIME, USER, ERROR, DEFAULT_PORT, \
    DEFAULT_ACCOUNT_NAME, DEFAULT_IP_ADDRESS, OK_DICT, ERROR_DICT


class TestClass(unittest.TestCase):
    def setUP(self):
        pass

    def tearDown(self):
        pass

    def test_presence(self):
        test = create_presence()
        test[TIME] = 1
        self.assertEqual(test, {ACTION: PRESENCE, TIME: 1, USER: {ACCOUNT_NAME: DEFAULT_ACCOUNT_NAME}})

    def test_dict_presence(self):
        self.assertIsInstance(create_presence(), dict)

    def test_no_str_presence(self):
        self.assertNotIsInstance(create_presence(), str)

    def test_no_presence(self):
        test = create_presence()
        test[TIME] = 1
        self.assertNotEqual(test, {ACTION: PRESENCE, TIME: 1})

    def test_200_ans(self):
        self.assertEqual(process_ans(OK_DICT), '200 : ok')

    def test_400_ans(self):
        self.assertEqual(process_ans(ERROR_DICT), '400 : Bad Request')

    def test_error_ans(self):
        self.assertRaises(ValueError, process_ans, 'Wrong dict')

    def test_str_ans(self):
        self.assertIsInstance(process_ans({RESPONSE: 200}), str)

    def test_no_dict_ans(self):
        self.assertNotIsInstance(process_ans({RESPONSE: 200}), dict)


if __name__ == '__main__':
    unittest.main()