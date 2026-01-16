'''Module to test the project2.SchoolSystem.data.msgrModel module'''
import unittest
from SchoolSystem.data.msgrModel import Messager
from SchoolSystem.data.logger import get_logger

_log = get_logger(__name__)

class MsgrModelTestSuite(unittest.TestCase):
    '''Test suite for Messager Class'''
    messenger = None
    def setUp(self):
        self.messenger = Messager(-1, 'msg_to', 'msg_from', 'msg_msg', 'msg_date')
    def tearDown(self):
        self.messenger = None
    @classmethod
    def setUpClass(cls):
        cls.messenger = Messager()
    @classmethod
    def tearDownClass(cls):
        cls.messenger = None

    def test_str(self):
        '''Tests __str__ in messenger'''
        _log.info('Testing test_str')
        self.messenger = Messager('msg_to', 'msg_from', 'msg_msg', 'msg_date')
        self.assertIs(type(str(MsgrModelTestSuite.messenger)), str)

    def test_to_dict(self):
        '''Test to_dict in messenger'''
        _log.info('Testing test_to_dict')
        self.messenger = Messager('msg_to', 'msg_from', 'msg_msg', 'msg_date')
        self.assertIs(type(MsgrModelTestSuite.messenger.to_dict()), dict)

    def test_from_dict(self):
        '''Test from_dict in messenger'''
        _log.info('Testing test_from')
        test_dict = {'msg_to':'msg_to', 'msg_from': 'msg_from',
                     'msg_msg': 'msg_msg', 'msg_date': 'msg_date'}
        self.messenger = Messager().from_dict(test_dict)
        self.assertIs(type(MsgrModelTestSuite.messenger), Messager)



if __name__ == '__main__':
    unittest.main()