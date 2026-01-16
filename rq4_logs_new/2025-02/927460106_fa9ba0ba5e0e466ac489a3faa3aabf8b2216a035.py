import unittest
from maze import Maze
from cell import Cell

class TestMaze(unittest.TestCase):
    def test_maze_create_cells(self):
        n_rows = 10
        n_cols = 20
        m1 = Maze(30, 30, n_rows, n_cols, 30, 20)
        self.assertEqual(
            len(m1._cells),
            n_cols
        )
        self.assertEqual(
            len(m1._cells[0]),
            n_rows
        )
        self.assertIsInstance(
            m1._cells[0][0],
            Cell
        )

    def test_small_maze_create_cells(self):
        n_rows = 2
        n_cols = 1
        m2 = Maze(30, 30, n_rows, n_cols, 30, 20)
        self.assertEqual(
            len(m2._cells),
            n_cols
        )
        self.assertEqual(
            len(m2._cells[0]),
            n_rows
        )
    
    def test_big_maze_create_cells(self):
        n_rows = 500
        n_cols = 1000
        m3 = Maze(30, 30, n_rows, n_cols, 30, 20)
        self.assertEqual(
            len(m3._cells),
            n_cols
        )
        self.assertEqual(
            len(m3._cells[0]),
            n_rows
        )

if __name__ == "__main__":
    unittest.main()