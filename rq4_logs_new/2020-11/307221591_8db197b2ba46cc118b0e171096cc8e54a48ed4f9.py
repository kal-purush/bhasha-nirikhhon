import sys
import unittest

import numpy as np

sys.path.append('./shared')
import activationfunction as af

class Test_Activation_Function(unittest.TestCase):

    def setUp(self):
        # init
        pass

    def tearDown(self):
        # dispose
        pass

    def test_sigmoid(self):
        self.assertEqual(af.sigmoid(-50), np.array(1e-15))
        self.assertEqual(af.sigmoid(50), np.array(1.0 - 1e-15))
        self.assertEqual(af.sigmoid(0), np.array(0.5))

    def test_relu(self):
        self.assertEqual(af.relu(-50), np.array(0))
        self.assertEqual(af.relu(50), np.array(50))
        self.assertEqual(af.relu(0), np.array(0))

if __name__ == "__main__":
    unittest.main()