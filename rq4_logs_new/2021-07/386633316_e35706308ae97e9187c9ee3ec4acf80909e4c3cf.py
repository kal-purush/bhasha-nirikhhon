import unittest
from function import InputPreprocess

# unit test case
  
class TestPenceEquivalent(unittest.TestCase):
    
    # test function to test equality of two value
    def test_positive(self):

        user_input = ["6", "75", "167p", "4p", "1.97", "£1.33", "£2", "£20", "£1.97p", "£1p", "£1.p", "001.61p", "6.235p", "£1.256532677p"]
        ans = [6, 75, 167, 4, 197, 133, 200, 2000, 197, 100, 100, 161, 624, 126]

        # error message in case if test case got failed
        message = "First value and second value are not equal!"

        for idx, val in enumerate (user_input):

            input_amount = InputPreprocess(val)

            self.assertEqual(input_amount, ans[idx], message)
       
    def test_negative(self):

        user_input = ["", "1x", "£1x.0p", "£p", "asdasda"]
        ans = "This is an invalid input"

        # error message in case if test case got failed
        message = "First value and second value are not equal!"

        for idx, val in enumerate (user_input):

            input_amount = InputPreprocess(val)

            self.assertEqual(input_amount, ans, message)

  
if __name__ == '__main__':
    unittest.main()


