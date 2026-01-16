import matplotlib.pyplot as plt
import numpy as np
 


def _showSp(_spectr):
    x = _spectr[:,0]
    plt.bar(x, _spectr[:,1], 0.1)
    plt.xlabel("chanel")
    plt.ylabel("inp amout")
    plt.show()
    
# _fname = input()
_fname = "../spectrum.dat"

_spectr = np.loadtxt(_fname)
# _crNS(_spectr, 2, _nsp)
_showSp(_spectr)
print(len(_spectr))
    