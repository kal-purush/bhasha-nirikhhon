# -*- coding: utf-8 -*-
"""
@author: Craig Thorburn
"""

import unittest

from entity_recognizer import ThisEntity


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.base_test = ThisEntity(threshold = 0, exceptions = [])
        self.exception_test = ThisEntity(threshold = 0, exceptions = ['is'])
        self.threshold_test = ThisEntity(threshold = 2, exceptions = [])
        
        self.base_test.add_sentence('This man did something')
        self.base_test.add_sentence('this boy did something')
        self.base_test.add_sentence('I will tell you that this woman did something')
        self.base_test.add_sentence('I will tell you that This girl did something')
        
        self.threshold_test.add_sentence('This man did something')
        self.threshold_test.add_sentence('this man did something')
        self.threshold_test.add_sentence('I will tell you that this woman did something')
        self.threshold_test.add_sentence('I will tell you that This girl did something')
        
        self.exception_test.add_sentence('This man did something')
        self.exception_test.add_sentence('this is doing something')
        self.exception_test.add_sentence('I will tell you that this woman did something')
        self.exception_test.add_sentence('I will tell you that This girl is doing something')


    def test_base(self):
        self.assertEqual(self.base_test.finalize_vocab(),5)
        self.assertEqual(self.base_test.get_entity_index('this man'),1)
        self.assertEqual(self.base_test.get_entity_index('this boy'),2)
        self.assertEqual(self.base_test.get_entity_index('this woman'),3)
        self.assertEqual(self.base_test.get_entity_index('this girl'),4)
        self.assertEqual(self.base_test.get_entity_index('this person'),0)
        
    def test_threshold(self):
        self.assertEqual(self.threshold_test.finalize_vocab(),2)
        self.assertEqual(self.threshold_test.get_entity_index('this man'),1)
        self.assertEqual(self.threshold_test.get_entity_index('this woman'),0)
        self.assertEqual(self.threshold_test.get_entity_index('this girl'),0)
        self.assertEqual(self.threshold_test.get_entity_index('this person'),0)
        
    def test_exceptions(self):
        self.assertEqual(self.exception_test.finalize_vocab(),4)
        self.assertEqual(self.exception_test.get_entity_index('this man'),1)
        self.assertEqual(self.exception_test.get_entity_index('this is'),0)
        self.assertEqual(self.exception_test.get_entity_index('this woman'),2)
        self.assertEqual(self.exception_test.get_entity_index('this girl'),3)
        
if __name__ == '__main__':
    unittest.main()