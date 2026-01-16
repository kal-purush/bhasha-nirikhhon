import numpy as np
from zernike import RZern
import matplotlib.pyplot as plt

cart = RZern(6)
# pol = RZern(6)
L, K = 200, 250
ddx = np.linspace(-1.0, 1.0, K)
ddy = np.linspace(-1.0, 1.0, L)
xv, yv = np.meshgrid(ddx, ddy)
cart.make_cart_grid(xv, yv)
c = np.zeros(cart.nk)

# pol.make_pol_grid(np.linspace(0.0, 1.0), np.linspace(0.0, 2*np.pi))
# c = np.zeros(pol.nk)

fig = plt.figure(1)

for i in range(1, 10):
    plt.subplot(3, 3, i)
    c *= 0.0
    c[i] = 1.0
    Phi = cart.eval_grid(c, matrix=True)
    # Phi = pol.eval_grid(c, matrix=True)
    plt.imshow(Phi, origin='lower', extent=(-1, 1, -1, 1))
    plt.show()
    # plt.polar(Phi)
    plt.axis('off')

plt.show()