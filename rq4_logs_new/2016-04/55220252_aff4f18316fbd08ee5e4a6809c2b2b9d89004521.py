import unittest
import nose

from stacks import Stack, check_brackets


class TestModels(unittest.TestCase):
    """Test class for Stack structure."""

    def setUp(self):
        """Set up test stack class."""
        self.stack = Stack()

    def test_stack_creation(self):
        """Test correct stack structure creation."""
        self.assertIsInstance(self.stack, Stack)

    def test_stack_checkbrackets(self):
        """Test working of check brackets function."""
        # correctly input brackets beginning of stack
        self.stack = '(babayao)'
        self.assertTrue(check_brackets(self.stack))

        # corrrectly input brackets within stack
        self.stack = 'baba(yao)'
        self.assertTrue(check_brackets(self.stack))

if __name__ == '__main__':
    nose.run()