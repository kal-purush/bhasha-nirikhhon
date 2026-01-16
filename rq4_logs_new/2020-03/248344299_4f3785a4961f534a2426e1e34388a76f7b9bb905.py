from scipy.optimize import linprog
from time import time
import numpy as np
import matplotlib.pyplot as plt


def line():
    print('----------------------------------\n')

'''
time_list = []
niter_list = []
N = range(2, 18)
for n in N:
    c = []
    A = np.zeros((n, n))
    b = []

    for j in range(1, n + 1):
        c.append(-1 * 10 ** (n - j))
    for i in range(1, n + 1):
        for j in range(1, i):
            A[i - 1][j - 1] = 2 * 10 ** (i - j)
        A[i - 1][i - 1] = 1
        b.append(100 ** (i - 1))
        c.append(0)
    A = np.concatenate((A, np.eye(n)), axis=1)

    start_time = time()

    res = linprog(c, A, b, method='revised simplex', options={'maxiter': 100000000000})

    stop_time = time()
    calc_time = stop_time - start_time

    time_list.append(calc_time)
    niter_list.append(res.nit)

    print(res)
    print('temps de calcul = ', calc_time, ' s')
    line()

plt.figure(1)
plt.plot(N, time_list)
plt.title('temps de calcul en fonction de N')
plt.xlabel('')
plt.figure(2)
plt.plot(N, niter_list, color='r')
plt.title("nombre d'itératons en fonction de N")
plt.show()
'''
# --------------------------------------------------------------------------------

time_list2 = []
niter_list2 = []
N2 = range(2, 18)
for n in N2:
    c2 = []
    A2 = np.zeros((n, n))
    b2 = []

    for j in range(1, n + 1):
        c2.append((1/100**(1-j)) * -1 * 10 ** (n - j))
    for i in range(1, n + 1):
        for j in range(1, i):
            A2[i - 1][j - 1] = (1/100**(1-j)) * 2 * 10 ** (i - j)
        A2[i - 1][i - 1] = 1 * (1/100**(1-i))
        b2.append(100 ** (i - 1))
        c2.append(0)
    A2 = np.concatenate((A2, np.eye(n)), axis=1)

    start_time2 = time()

    res2 = linprog(c2, A2, b2, method='revised simplex', options={'maxiter': 100000000000})

    stop_time2 = time()
    calc_time2 = stop_time2 - start_time2

    time_list2.append(calc_time2)
    niter_list2.append(res2.nit)

    print(res2)
    print('temps de calcul = ', calc_time2, ' s')
    line()

plt.figure(1)
plt.plot(N2, time_list2)
plt.title('temps de calcul en fonction de N (avec changement de variable)')
plt.xlabel('')
plt.figure(2)
plt.plot(N2, niter_list2, color='r')
plt.title("nombre d'itératons en fonction de N (avec changement de variable")
plt.show()