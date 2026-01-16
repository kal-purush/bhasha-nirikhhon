import unittest
import main


class TestMaze(unittest.TestCase):

    def setUp(self) -> None:
        self.maze_1 = main.read_maze('maze1.txt')
        self.maze_2 = main.read_maze('maze2.txt')
        self.maze_3 = main.read_maze('maze3.txt')
        main.num_paths = 0

    def tearDown(self) -> None:
        self.maze_1 = None
        self.maze_2 = None
        self.maze_3 = None
        main.num_paths = 0

    def test_check_next(self):
        for l in range(len(self.maze_1)):
            main.check_next(self.maze_1, (l, 0))
        self.assertEqual(main.num_paths, 5, "Wrong number of paths")
        main.num_paths = 0
        for l in range(len(self.maze_2)):
            main.check_next(self.maze_2, (l, 0))
        self.assertEqual(main.num_paths, 2, "Wrong number of paths")
        main.num_paths = 0
        for l in range(len(self.maze_3)):
            main.check_next(self.maze_3, (l, 0))
        self.assertEqual(main.num_paths, 201684, "Wrong number of paths")

    def test_check_exit(self):
        self.assertTrue(main.check_exit(self.maze_1, (0, 2)))
        self.assertFalse(main.check_exit(self.maze_1, (0, 1)))
        self.assertTrue(main.check_exit(self.maze_2, (0, 9)))
        self.assertFalse(main.check_exit(self.maze_2, (1, 1)))
        self.assertTrue(main.check_exit(self.maze_3, (0, 6)))
        self.assertFalse(main.check_exit(self.maze_3, (4, 5)))