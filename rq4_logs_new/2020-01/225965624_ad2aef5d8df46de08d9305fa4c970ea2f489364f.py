import unittest
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


def Fibonacci(n):
    if n < 0:
        print("pLease enter proper input")

    elif n == 1:
        return 0

    elif n == 2:
        return 1
    else:
        return Fibonacci(n - 1) + Fibonacci(n - 2)




def convertNumbertoString(num):
    below20 = ['Zero', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten', 'Eleven',
                'Twelve', 'Thirteen', 'Fourteen', 'Fifteen', 'Sixteen', 'Seventeen', 'Eighteen', 'Nineteen']
    tenthposition = ['Twenty', 'Thirty', 'Forty', 'Fifty', 'Sixty', 'Seventy', 'Eighty', 'Ninety']
    above100 = {100: 'Hundred', 1000: 'Thousand', 1000000: 'Million', 1000000000: 'Billion'}

    if num < 20:
        return below20[num]

    if num < 100:
        return tenthposition[(int)(num / 10) - 2] + ('' if num % 10 == 0 else ' ' + below20[num % 10])


    position = max([key for key in above100.keys() if key <= num])

    return convertNumbertoString((int)(num / position)) + ' ' + above100[position] + (
        '' if num % position == 0 else ' ' + convertNumbertoString(num % position))







class Challenge4(unittest.TestCase):

    def setUp(self):
        print ""


    def tearDown(self):
        print ""

    def test_challenge4(self):
        fib_num = (Fibonacci(9))
        print fib_num

        fib_num = (Fibonacci(8))
        print str(fib_num) + ' - ' + convertNumbertoString(fib_num)
        fib_num = (Fibonacci(13))
        print str(fib_num) + ' - ' + convertNumbertoString(fib_num)
        fib_num = (Fibonacci(21))
        print str(fib_num) + ' - ' + convertNumbertoString(fib_num)




if __name__ == '__main__':
    unittest.main()