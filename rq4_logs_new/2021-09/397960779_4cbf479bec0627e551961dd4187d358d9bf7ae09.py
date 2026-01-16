import unittest
import pathlib


from diary_tool_main.diary_tool import set_log_file_path

class TestSetLogFilePath(unittest.TestCase):
    def test_set_log_file_path(self):
        """
        Test function sets log file path correctly
        """

        # actual path to function
        main_path = set_log_file_path()

        current_directory = str(pathlib.Path(__file__).parent.resolve())

        # our fabricated path to test against actual path in main
        func_path = current_directory.replace(
            'tests', 'diary_tool_main/diary_log')

        self.assertEqual(main_path, func_path)

if __name__ == '__main__':
    unittest.main()

    
    