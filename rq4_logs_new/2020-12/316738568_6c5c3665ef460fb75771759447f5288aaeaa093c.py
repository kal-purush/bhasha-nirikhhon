from unittest import TestCase


class Test(TestCase):
    def test_find_entry_pair(self):
        from src.day1_1 import find_entry_pair
        input_list = [1721, 979, 366, 299, 675, 1456]
        correct_answer = 514579
        self.assertTrue(find_entry_pair(input_list) == correct_answer)

    def test_find_entry_triplet(self):
        from src.day1_2 import find_entry_triplet
        input_list = [1721, 979, 366, 299, 675, 1456]
        correct_answer = 241861950
        self.assertTrue(find_entry_triplet(input_list) == correct_answer)