# -*- coding: utf-8 -*-

import unittest
import pandas as pd
from AnalysisPackage import *

class testAnalysis(unittest.TestCase):
    def test_is_clean_correct(self):
        sentence1 = 'The weather is very good, but the location is bad.'
        answer1 = 'weather good location bad'
        sentence2 = 'I love this hotel! The staff is nice and the room is very big. Besides, the \
                        breakfast has many choices!'
        answer2 = 'love hotel staff nice room big besides breakfast many choice'
        self.assertEqual(clean(sentence1),answer1)
        self.assertEqual(clean(sentence2),answer2)
            
    def test_get_word_freq(self):
        d = ({'name':['A','A','A','A','A','B','B'],
              'pos':['good','perfect','awsome','good','good','awsome','amazing'],
              'neg':['bad','terrible','bad','horrible','sad','terrible','terrible']})
        df = pd.DataFrame(d)
        answer = get_keyword_freq(df)
        self.assertIsInstance(answer,pd.core.frame.DataFrame)
        n1 = answer[answer['Keyword']=='good']['Frequency']
        self.assertTrue(list(n1)[0] == 3)                  # Gets the value of n1
        n2 = answer[answer['Keyword']=='amazing']['Frequency']
        self.assertTrue(list(n2)[0] == 1)                  # Gets the value of n2
    
    def test_get_distance(self):
        d = ({'name':['A','B','C','D','E'],
              'x':[1,2,3,4,5],
              'y':[1,2,3,4,4],
              'z':[0,1,2,3,4],
              'o':[1,2,3,3,4],
              'p':[1,3,4,5,1],
              'q':[4,5,6,3,2],
              'Label':[1,0,0,1,1]})
        df = pd.DataFrame(d)
        dic = get_distance('A',df)
        self.assertEqual(dic['E'],5.0)
        self.assertFalse('B' in dic.keys())
        self.assertFalse('C' in dic.keys())
    
if __name__ == '__main__':
    unittest.main()