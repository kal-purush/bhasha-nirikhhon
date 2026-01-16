import ctypes
import unittest

class TestBitwise(unittest.TestCase):
    def test_lowest_one(self):
        lib = ctypes.cdll.LoadLibrary("./bitwise.o") 
        x = 104
        res = lib.last_one(x)
        self.assertEqual(res, 8)
