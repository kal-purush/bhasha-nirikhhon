#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from unittest import TestCase

from logger import merge, log


class TestMerge(TestCase):
    def test_exec(self):
        path = "tests/logs"
        ret = merge.Merge().exec(path)
        self.assertTrue(ret)
        os.remove(path + merge.Merge.FILE_SUFFIX)

    def test_exec__dir_not_exists(self):
        path = "tests/not_exixts_dir"
        log.set_level(log.Level.DEBUG)
        ret = merge.Merge().exec(path)
        self.assertFalse(ret)