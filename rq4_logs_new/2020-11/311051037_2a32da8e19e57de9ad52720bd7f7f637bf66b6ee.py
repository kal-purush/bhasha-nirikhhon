import unittest
import random

import encryption

PASSWORDCHARS = '+-/*!&$#?=@<>abcdefghijklnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'
STRCHARS = ' abcdefghijklnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'


# Encryption module test
class TestEncryption(unittest.TestCase):
    def test_simple_string(self):
        password = 'D&EXHb(48v'
        str_for_test = 'String for test!!!111'

        cipher = encryption.AESCipher(password)

        encrypted = cipher.encrypt(str_for_test)
        result = cipher.decrypt(encrypted)

        self.assertEqual(str_for_test, result)

    def test_simple_string_with_random_password_string(self):
        password = ''
        for i in range(20):
            password += random.choice(PASSWORDCHARS)
        str_for_test = ''
        for i in range(20):
            str_for_test += random.choice(STRCHARS)

        cipher = encryption.AESCipher(password)

        encrypted = cipher.encrypt(str_for_test)
        result = cipher.decrypt(encrypted)

        self.assertEqual(str_for_test, result)


# Запустить тестирование
if __name__ == '__main__':
    unittest.main()