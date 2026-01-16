'''
Created on 29 dec. 2015

@author: Ruud
'''
import unittest

import xdrlib2

class TestOpaque(unittest.TestCase):
    class FixedLengthOpaque(xdrlib2.FixedOpaque):
        size = 5
    
    class VarLengthOpaque(xdrlib2.VarOpaque):
        size = 9


    def test_fixed_length_opaque(self):
        blob = self.FixedLengthOpaque(b'\0\xff\xab\xcd\x01')
        bp = blob.pack()
        self.assertEqual(bp, b'\0\xff\xab\xcd\x01\0\0\0')
        self.assertEqual(self.FixedLengthOpaque.unpack(bp), blob)
    
    
    def test_fixed_length_opaque_errors(self):
        self.assertRaises(ValueError, self.FixedLengthOpaque, b'123')
        self.assertRaises(ValueError, self.FixedLengthOpaque, b'1234567890')
    
    def test_fixed_length_opaque_unpack_errors(self):
        self.assertRaises(ValueError, self.FixedLengthOpaque.unpack, b'123')
        self.assertRaises(ValueError, self.FixedLengthOpaque.unpack, b'1234567890')
    
    def test_var_length_opaque(self):
        blob = self.VarLengthOpaque(b'\0\xff\xab\xcd\x01')
        bp = blob.pack()
        self.assertEqual(bp, b'\0\0\0\x05\0\xff\xab\xcd\x01\0\0\0')
        self.assertEqual(self.VarLengthOpaque.unpack(bp), blob)
        
    def test_var_length_opaque_errors(self):
        self.assertRaises(ValueError, self.VarLengthOpaque, b'1234567890')
    
    def test_var_length_opaque_unpack_errors(self):
        self.assertRaises(ValueError, self.VarLengthOpaque.unpack, b'\0\0\0\x05123')
        self.assertRaises(ValueError, self.VarLengthOpaque.unpack, b'\0\0\0\x0a1234567890\0\0\0')
    


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()