import os, sys 
from matplotlib import pyplot as plt
import numpy as np

a=np.load('results/gate0_numblock_512_meff_1024_offsetn_0_offsetm_0_uc.npy')
a=np.reshape(a, (512,1024))
a_real = np.real(a)
print np.amax(a_real), np.amin(a_real)
a_imag = np.imag(a)

print a_real
print a_imag

plt.figure()
plt.subplot(1,2,1)
plt.imshow(a_real,origin='lower')
plt.colorbar()
plt.title("Real part",y=1.02)

plt.subplot(1,2,2)
plt.imshow(a_imag,origin='lower')
plt.colorbar()
plt.title("Imaginary part",y=1.02)

plt.tight_layout()

plt.savefig('viswesh_result.png', bbox_inches='tight')
plt.close()