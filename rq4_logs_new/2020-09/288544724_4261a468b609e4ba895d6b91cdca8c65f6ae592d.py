import unittest, sys
sys.path.append('..')
from wordbreak import Solution

class TestWordBreak(unittest.TestCase):
    
    def setUp(self):
        self.s = Solution()
        
    def test_word_break(self):
        case_one = {'s': 'leetcode',
                    'wordDict': ['leet','code']}
        
        self.assertTrue(self.s.wordBreak(**case_one))
        
        case_two = {'s': 'applepenapple',
                    'wordDict': ['apple','pen']}
        
        self.assertTrue(self.s.wordBreak(**case_two))
        
        case_three = {'s': 'catsandog',
                      'wordDict': ['cats','dog','sand','and','cat']}
        
        self.assertFalse(self.s.wordBreak(**case_three))
        
        case_four = {'s': 'a',
                     'wordDict': ['aa']}
        
        self.assertFalse(self.s.wordBreak(**case_four))
        
if __name__ == '__main__':
    unittest.main()