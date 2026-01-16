import useful_functions as uf
import numpy as np
#from Udacity deep learn


print(uf.softmax(scores))

# Plot sm.softmax curves
import matplotlib.pyplot as plt
x = np.arange(-2.0, 6.0, 0.1)
scores = np.vstack([x, np.ones_like(x), 0.2 * np.ones_like(x)])

plt.plot(x, uf.softmax(scores).T, linewidth=2)
plt.show()