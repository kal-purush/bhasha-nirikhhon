import unittest
import os
from unittest.case import expectedFailure
from diary_tool import ask_and_await_answer, get_datetime_as_string, set_log_file_path

class TestGetQuestionsFromUser(unittest.TestCase):
    def test_get_questions_from_user(self):
        """
        Tests logic for function ask_and_await_answer
        """

        datetime = get_datetime_as_string()

        # opens file as file object using append parameter
        current_file_path = os.path.realpath(__file__)
        test_file_path = current_file_path.replace('test_ask_and_await_answer.py', 'test_file_for_ask_and_await_answer')
                
        question_list = ['Question 1', 'Question 2',]

        for i in range(0, len(question_list)):

            answer = ('\n\n' + question_list[i] + '\n\n' + 'answer' + str(i+1))

            # outputs answer to questions in correct format
            # to log file
            with open(test_file_path, 'a') as file:

                file.write('\n\n' '\n\n'
                                + question_list[i] + '\n\n' + answer + '\n\n')
        
        expected_string = ("\n\n \n\n Question 1 \n\n answer 1 \n\n"
                            "\n\n \n\n Question 2 \n\n answer 2 \n\n")

        # assert statements here
        with open(test_file_path) as file:
            self.assertEqual(file.read(), expected_string)


        # clears the test file after writing and assertion        
        open(test_file_path, 'w').close()



if __name__ == '__main__':
    unittest.main()