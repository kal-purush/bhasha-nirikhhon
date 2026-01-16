import numpy as np
import matplotlib.pyplot as pl

runs = 100000
p = np.random.uniform(30, 60, runs)
c = np.random.uniform(1, 10, runs)

pl.hist(p+c)
pl.show()

