import unittest

from autowordl import score

class TestScore(unittest.TestCase):
    def test_nomatch(self):
        self.assertEqual(score('BATHE','SPOON'), '.....')
    
    def test_fullmatch(self):
        self.assertEqual(score('TRYST', 'TRYST'), 'TRYST')

    def test_multiple(self):
        # this was an error in the old code
        self.assertEqual(score('DRINK', 'DANDY'), 'D..n.')

if __name__ == "__main__":
    unittest.main()