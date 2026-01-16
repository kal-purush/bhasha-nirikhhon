import unittest
import squarematice
from squarematice import SquareMatrix


class TestSquareMatrice(unittest.TestCase):
    def test_transposition(self):
        x = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        y = SquareMatrix([[1, 4, 7], [2, 5, 8], [3, 6, 9]])
        self.assertEqual(x.transposition(), y)
        x1 = SquareMatrix([[1, 2], [4, 5]])
        y1 = SquareMatrix([[1, 4], [2, 5]])
        self.assertEqual(x1.transposition(), y1)
        x2 = SquareMatrix([[1]])
        y2 = SquareMatrix([[1]])
        self.assertEqual(x2.transposition(), y2)

    def test_add(self):
        x = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        y = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        z = SquareMatrix([[2, 4, 6], [8, 10, 12], [14, 16, 18]])
        self.assertEqual(x + y, z)
        x1 = SquareMatrix([[1, 2], [8, 9]])
        y1 = SquareMatrix([[-1, -2], [-8, -9]])
        z1 = SquareMatrix([[0, 0], [0, 0]])
        self.assertEqual(x1 + y1, z1)
        with self.assertRaises(TypeError):
            x3 = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            y3 = 1
            x3 + y3
        with self.assertRaises(ValueError):
            x4 = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            y4 = SquareMatrix([[1, 2], [4, 5]])
            x4 + y4

    def test_subtract(self):
        x = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        y = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        z = SquareMatrix([[0, 0, 0], [0, 0, 0], [0, 0, 0]])
        self.assertEqual(x - y, z)
        x1 = SquareMatrix([[1, 2], [8, 9]])
        y1 = SquareMatrix([[-1, -2], [-8, -9]])
        z1 = SquareMatrix([[2, 4], [16, 18]])
        self.assertEqual(x1 - y1, z1)
        with self.assertRaises(TypeError):
            x3 = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            y3 = 1
            x3 - y3
        with self.assertRaises(ValueError):
            x4 = SquareMatrix([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            y4 = SquareMatrix([[1, 2], [4, 5]])
            x4 - y4


if __name__ == '__main__':
    unittest.main()