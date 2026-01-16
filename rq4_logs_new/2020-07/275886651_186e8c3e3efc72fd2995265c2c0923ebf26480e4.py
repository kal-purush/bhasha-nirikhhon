'''Module to test the project2.SchoolSystem.users.model module'''
import unittest

from SchoolSystem.assignments.model import Assignment

from SchoolSystem.data.logger import get_logger

_log = get_logger(__name__)

class AssignmentTestSuite(unittest.TestCase):
    '''Test suite for User Class'''
    assignment = None
    def setUp(self):
        self.assignment = Assignment(-1, 'jill', 'description')
    def tearDown(self):
        self.assignment = None
    @classmethod
    def setUpClass(cls):
        cls.assignment = Assignment()
    @classmethod
    def tearDownClass(cls):
        cls.assignment = None

    def test_get_id(self):
        '''Tests retrieval of get_id'''
        _log.info('Testing test_get_id')
        self.assertEqual(-1, AssignmentTestSuite.assignment.get_id())

    def test_set_id(self):
        '''Tests to see if set_id works'''
        _log.info('Testing test_set_id')
        AssignmentTestSuite.assignment.set_id(-10)
        self.assertEqual(-10, AssignmentTestSuite.assignment._id)

    def test_str(self):
        '''Tests __str__ in assignment'''
        _log.info('Testing test_str')
        self.assignment = Assignment('name', 'description')
        self.assertIs(type(str(AssignmentTestSuite.assignment)), str)

    def test_to_dict(self):
        '''Test to_dict in assignment'''
        _log.info('Testing test_to_dict')
        self.assignment = Assignment('name', 'description')
        self.assertIs(type(AssignmentTestSuite.assignment.to_dict()), dict)

    def test_from_dict(self):
        '''Test from_dict in assignment'''
        _log.info('Testing test_from')
        test_dict = {'name': 'name', 'description': 'description',}
        self.user = Assignment().from_dict(test_dict)
        self.assertIs(type(AssignmentTestSuite.assignment), Assignment)



if __name__ == '__main__':
    unittest.main()