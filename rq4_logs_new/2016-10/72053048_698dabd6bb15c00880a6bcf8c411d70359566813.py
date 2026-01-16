# -*- coding: utf-8 -*-
import random
import unittest

from selenium import webdriver


class CalculatorTest(unittest.TestCase):
    def setUp(self):
        self.driver = webdriver.Firefox()
        self.driver.implicitly_wait(30)
        self.base_url = "http://chemistry.about.com"

    def test_operator(self):
        num1 = random.randrange(0, 10)
        num2 = random.randrange(0, 10)

        operators = ("mul", "div", "plus", "minus")
        test_operator = random.choice(operators)

        if test_operator == "mul":
            expected_result = num1 * num2
        elif test_operator == "plus":
            expected_result = num1 + num2
        elif test_operator == "minus":
            expected_result = num1 - num2
        else:
            if num2 == 0:
                expected_result = "Infinity"
            else:
                expected_result = num1 / num2



        expected_page_title = "Online Calculator"

        driver = self.driver
        driver.get(self.base_url + "/library/weekly/blcalculator.htm")

        ################ assert driver.title == expected_page_title

        actual_page_title = driver.title

        self.assertEqual(
                expected_page_title, actual_page_title,
                "Expected the title of the page at "
                "{0} to be {1} but it was {2} instead".format(
                        driver.current_url,
                        expected_page_title,
                        actual_page_title
                ))
        driver.find_element_by_name("cal" + str(num1)).click()
        driver.find_element_by_name("cal{0}".format(test_operator)).click()
        driver.find_element_by_name("cal{0}".format(num2)).click()
        driver.find_element_by_name("calequal").click()

        print num1, test_operator, num2, "is", expected_result

        actual_result = driver.find_element_by_name("calcResults").get_attribute("value")

        message = "Expected result to be {expected} but it was {actual}".format(
            expected=expected_result,
            actual=actual_result
        )
        self.assertEqual(str(expected_result), actual_result, message)


    def tearDown(self):
        self.driver.quit()


if __name__ == "__main__":
    unittest.main()