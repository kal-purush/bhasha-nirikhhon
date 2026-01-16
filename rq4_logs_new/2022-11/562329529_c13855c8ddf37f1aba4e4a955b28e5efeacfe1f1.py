In: import numpy as np
Out:

In: a = np.array([[1, 6],
                     [2, 8],
                     [3, 11],
                     [3, 10],
                     [1, 7]])
Out:

In: a
Out:   array([[ 1,  6],
              [ 2,  8],
              [ 3, 11],
              [ 3, 10],
              [ 1,  7]])

In: mean_a = np.mean(a, axis = 0)
       print(mean_a)
Out:   [2.  8.4]