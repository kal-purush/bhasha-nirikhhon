import unittest
from career import Career


class TestCareer(unittest.TestCase):

    def setUp(self) -> None:
        self.career_1 = Career("graph_1.txt")
        self.career_2 = Career("graph_2.txt")
        self.career_3 = Career("graph_3.txt")

    def tearDown(self) -> None:
        self.career_1 = None
        self.career_2 = None
        self.career_3 = None

    def test_find_optimal_path(self) -> None:
        self.assertEqual(self.career_1.find_optimal_path(), 12, "Not optimal")
        self.assertEqual(self.career_2.find_optimal_path(), 9999, "Not optimal")
        self.assertEqual(self.career_3.find_optimal_path(), 3, "Not optimal")