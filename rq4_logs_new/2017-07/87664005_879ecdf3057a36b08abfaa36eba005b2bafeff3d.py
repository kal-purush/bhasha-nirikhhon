#!/usr/bin/python

import os
import pathlib
import unittest

import popak_installer as pi


class TestPopakInstaller(unittest.TestCase):

    def test_backup(self):
        # create a file
        f = open(TEST_FILE, 'w')

        # backup the file
        pi.backup(f.name)

        # check that the backup file now exists
        file_path = pi.name_backup_file(TEST_FILE)
        test_bak = pathlib.Path(file_path)
        self.assertTrue(test_bak.is_file(), "expected backup file '" + test_bak.name + "' to exist but it's missing")

        # backup the file again
        file_path = pi.name_backup_file(TEST_FILE)
        test_bak2 = pathlib.Path(file_path)

        # check that test_file.backup2 exists
        expected_name = test_bak.name + "2"

        self.assertEqual(test_bak2.name, expected_name,
                         "expected backup file name '" + expected_name + "' but was " + test_bak2.name)

        self.assertTrue(test_bak2.is_file(),
                        "expected backup file '" + test_bak2.name + "' to exist but it's missing")

        # clean files
        os.remove(test_bak.name)
        os.remove(test_bak2.name)


TEST_FILE = 'test_file'