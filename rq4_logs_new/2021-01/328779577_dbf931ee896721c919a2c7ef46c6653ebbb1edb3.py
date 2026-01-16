"""Basic unit tests for lambdata"""

import unittest
import math
import pandas as pd 
import helper_functions as hf 

df = pd.read_csv('data/kep_observations.csv')

h = hf.Helper(df)

class TestLambdata(unittest.TestCase):
    """Test Helper Functions Package"""

    def test_null_count(self):
        """Testing null_count"""
        
        self.assertEqual(h.null_count(), len(df) * len(df.columns) - df.count().sum())
       

    def test_train_test_split(self):
        """Testing test_train_test_split"""
        
        train, test = h.train_test_split(.8)

        self.assertEqual(len(train), math.floor(len(df) * .8))
        self.assertEqual(len(test), math.ceil(len(df) * .2))

    def test_randomize(self):
        """Testing randomize"""
        
        rnd_df = h.randomize(42)
        
        dfi = df.index[0]
        ri = rnd_df.index[0]
        self.assertNotEqual(dfi, ri)


if __name__ == '__main__':
    unittest.main()