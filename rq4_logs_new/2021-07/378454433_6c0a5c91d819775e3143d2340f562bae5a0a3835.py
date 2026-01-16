import unittest
from unittest import mock
from pathlib import Path
import os
from pyfakefs.fake_filesystem_unittest import TestCase
from parameterized import parameterized
from resolver import find_script_with_name, InvalidScriptNameError, ConflictingScriptNamesError

def using_environment(variables):
    def wrapper(func):
        return mock.patch.dict(os.environ, variables)(func)
    return wrapper

class TestFindScriptWithName(TestCase):
    
    def setUp(self):
        self.setUpPyfakefs()
    
    @parameterized.expand([
        ('powershell', '/scripts_folder/script.ps1'),
        ('python', '/scripts_folder/script.py'),
        ('shell', '/scripts_folder/script.sh'),
        ('javascript', '/scripts_folder/script.js')
    ])
    @using_environment({"SCRIPTER_SCRIPTS": "/scripts_folder"})
    def test_the_correct_path_is_returned(self, name, script_path):
        self.fs.create_file(script_path)
        self.assertEqual(find_script_with_name('script'), Path(script_path))

    @using_environment({"SCRIPTER_SCRIPTS": "/scripts_folder"})
    def test_error_thrown_when_no_file_with_name(self):
        self.assertRaises(InvalidScriptNameError, find_script_with_name, 'script')

    @using_environment({"SCRIPTER_SCRIPTS": "/scripts_folder"})
    def test_error_thrown_when_multiple_files_with_name(self):
        self.fs.create_file('/scripts_folder/script.ps1')
        self.fs.create_file('/scripts_folder/script.py')
        self.assertRaises(ConflictingScriptNamesError, find_script_with_name, 'script')

if __name__ == '__main__':
    unittest.main()