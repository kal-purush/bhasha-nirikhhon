import matplotlib.pyplot as plt
import numpy as np
import random
from numpy.fft import rfft
from numpy.fft import rfft


nux = 0.169906324246756796829682956829568
delta = 0.010003
nuz = nux + delta


psix = 2*np.pi*0.25*nux
psiz = 2*np.pi*0.25*nuz

k1 = 10**(-7)
k2 = 10**(-5)
N = 800

Ax = 0.3342
Ay = 0.612

B = Ax/10 #level of noise

phi_y0 = 0.1213

turn = range(N)
n = np.array(turn)

x1 = Ax*np.cos(2*np.pi*nux*n)*np.exp(-k1*n**2)*np.exp(-k2*n)
px1 = -Ax*(2*np.pi*nux)*np.sin(2*np.pi*nux*n)
y1 = Ay*np.cos(2*np.pi*nuz*n + phi_y0)
py1 = -Ay*(2*np.pi*nuz)*np.sin(2*np.pi*nuz*n + phi_y0)

x2 = Ax*np.cos(2*np.pi*nux*n + psix)
px2 = -Ax*(2*np.pi*nux)*np.sin(2*np.pi*nux*n + psix)
y2 = Ay*np.cos(2*np.pi*nuz*n + phi_y0 + psiz)
py2 = -Ay*(2*np.pi*nuz)*np.sin(2*np.pi*nuz*n + phi_y0 + psiz)

x_n = list()
for elem in x1:
    x_n.append(elem + B*random.uniform(-1,1))

T =  int(1/nux) +1

x_p = abs(np.array(x_n))
cos = np.cos(2*np.pi*nux*n)
A = (1/N)*np.dot(cos,x1)
#print(A)
#print(Ax/A)
#print(np.mean(abs(cos)))

x_mean = list()
x_exp = list()
n_mean = list()
for elem in range(1, int(N/T)-1):
    x_mean.append(np.mean(x_p[elem*T:(elem+1)*T]))
    x_exp.append(max(x_p[elem*T:(elem+1)*T]))
    n_mean.append(elem*T)


y = abs(rfft(x_n))
Q = np.linspace(0,0.5, len(y))

print(np.pi/N)

#print(len(Q))
#print(len(y))
#print(len(rfft(x1)))

'''
plt.figure()

#plt.scatter(n_mean, x_mean)
#plt.plot(n_mean, T*np.array(x_mean))
plt.plot(x_n, color = 'b')
plt.plot(n_mean, x_exp, color = 'r')

plt.figure()

nn = np.array(n_mean)
plt.scatter(n_mean, x_exp - Ax*np.exp(-k1*nn**2)*np.exp(-k2*nn), color = 'b')

plt.figure()
plt.plot(Q,abs(y))
'''

plt.figure()

plt.plot(Q,y)
plt.plot([0.,0.5],[max(y)/2.,max(y)/2.])




plt.show()