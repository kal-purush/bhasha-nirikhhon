import sys

import numpy as np

sys.path.append('./cnn')
import cnn_analysistool as catool

def test_kernel_move():
    y = [[9, 0, 8, 1, 7, 2, 6, 3, 5, 4], [9, 0, 8, 1, 7, 2, 6, 3, 5, 4], [9, 0, 8, 1, 7, 2, 6, 3, 5, 4]]
    catool.kernelmove(y)