import numpy as np
import math
from matplotlib import pyplot as plt

x_vec = np.arange(0, 2*math.pi, math.pi/100)
sin_x = np.sin(x_vec)
cos_x = np.cos(x_vec)

plt.plot(x_vec, sin_x, color='b', label='Sine')
plt.plot(x_vec, cos_x, color='r', label='Cosine')

plt.title('Basic Trig Functions')
plt.xlabel('x')
plt.ylabel('F(X)')
plt.ylim([-2,2])
plt.legend()

plt.show()