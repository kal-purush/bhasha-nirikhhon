import unittest
from helpers.validator import Validator

class TestCpfValidator(unittest.TestCase):

    def setUp(self):
        pass

    def test_return_true_when_cpf_is_valid(self):
        valid_cpf = '35818079805'
        is_valid = Validator.cpf_validator(valid_cpf)
        self.assertTrue(is_valid)

    def test_return_false_when_cpf_is_invalid(self):
        invalid_cpf = 'aaaaaaaaa'
        is_valid = Validator.cpf_validator(invalid_cpf)
        self.assertFalse(is_valid)


if __name__ == '__main__':
    unittest.main()