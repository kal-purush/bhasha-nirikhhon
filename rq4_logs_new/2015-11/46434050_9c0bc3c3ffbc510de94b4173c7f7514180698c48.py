from matplotlib import pyplot as plt
from numpy import log
from scipy.optimize import brentq as findroot
import seaborn as sns


class SimpleModel:
    def u(self, c, n):
        "Utility function"
        return ((((c ** (1 - self.sigma)) / (1 - self.sigma) if self.sigma != 1
                  else log(c))) - self.psi * (n ** (1 + self.gamma)) /
                (1 + self.gamma))

    def u_prime_c(self, c):
        "Derivative of utility function"
        return c ** (-self.sigma)

    def u_inverse(self, x):
        return x ** (- 1 / self.sigma)

    def u_prime_n(self, n):
        "Inverse of derivative of utility function"
        return - self.psi * n ** self.gamma

    def u_prime_of_n_inverse(self, x):
        return (x / (- self.psi)) ** (1 / self.gamma)

    def __init__(self, parameters):
        try:
            self.beta = parameters['be']
            self.r = parameters['r']
            self.sigma = parameters['sigma']
            self.gamma = parameters['gamma']
            self.psi = parameters['psi']
            self.g = parameters['g']
            self.R = self.r + 1
            self.bR = self.R * self.beta
        except KeyError as k:
            print("Parameter error. Expecting " + str(k))
            raise


def multiplier(eta):
    x = findroot(lambda l: m.u(m.u_inverse((l + eta *
                                           (1 - m.sigma)) ** (-1)),
                               m.u_prime_of_n_inverse(
                                - (l + eta * (1 + m.gamma)) **
                                (-1))) - udev,
                 0.001, 10)
    return x


def c_at_w(eta):
    return m.u_inverse((multiplier(eta) + eta * (1 - m.sigma)) ** (-1))


def n_at_w(eta):
    return m.u_prime_of_n_inverse(
                - (multiplier(eta) + eta * (1 + m.gamma)) ** (-1))


def c_before_w(mult, eta):
    return m.u_inverse((mult + eta * (1 - m.sigma)) ** (-1))


def n_before_w(mult, eta):
    return m.u_prime_of_n_inverse(-(mult + eta * (1 + m.gamma)) ** (-1))


def tax(c, n):
    return - 1 - m.u_prime_c(c) / m.u_prime_n(n)


# PARAMETERS

ssdebt = .5  # steady state debt
r = 0.05
g = 0.2
par = {'r': r,
       'be': .94,
       'sigma': 1,
       'gamma': .5,
       'g': g,
       'psi':  1 / (1 - g - r * ssdebt)}
pTminus = 10
pTplus = 10

m = SimpleModel(par)
udev = m.u(1 - m.g - m.r * ssdebt, 1)

bigA_ss = m.R * (c_at_w(0) + m.g - n_at_w(0)) / m.r
a_ss = (c_at_w(0) - n_at_w(0)) / (1 - m.beta)
eta0 = eta1 = .18

c_list = []
n_list = []
cap_t = 500
for t in range(cap_t):
    c_list.append(c_at_w(eta1))
    n_list.append(n_at_w(eta1))
    eta1 = eta1 * m.bR
    print(t)
c_before = []
n_before = []
l1 = multiplier(eta0)
eta1 = eta0
for t in range(cap_t):
    l1 = l1 / m.bR
    eta1 = eta1 / m.bR
    c_before.append(c_before_w(l1, eta1))
    n_before.append(n_before_w(l1, eta1))
c_before.reverse()
n_before.reverse()
c_list = c_before + c_list
n_list = n_before + n_list
big_A = [bigA_ss]
a = [a_ss]
for j in range(len(c_list)):
    c_item, n_item = c_list[-j-1], n_list[-j-1]
    cprime = c_list[-j] if j > 0 else c_list[-1]
    big_A.append(c_item + m.g - n_item + big_A[-1] / m.R)
    a.append(c_item + m.u_prime_n(n_item) / m.u_prime_c(c_item) *
             n_item + m.beta * m.u_prime_c(cprime) / m.u_prime_c(c_item) *
             a[-1])
a.reverse()
big_A.reverse()

sns.set_style("white")  # optional seabird settings
sns.set_palette('Greys_d')  # optional seabird settings
plt.rc('text', usetex=True)
plt.rc('font', **{'family': 'sans-serif',
                  'sans-serif': ['Computer Modern Roman']})

fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(10, 7))
axes[0, 0].plot(c_list[cap_t-pTminus:cap_t+pTplus], 'k-',
                lw=2, label='$c$')
axes[0, 0].plot(n_list[cap_t-pTminus:cap_t+pTplus], 'k--',
                lw=2, label='$n$')
axes[0, 0].legend(('$c$', '$n$'), loc=2, fontsize=14)
axes[0, 0].set_title('(a) Consumption and labor')
axes[0, 0].set_xlabel('')
axes[0, 0].set_ylim([0.55, 0.85])
axes[0, 0].set_xticks([0, 5, 10, 15, 20])
axes[0, 0].set_xticklabels([0, 5, 'T', 15, 20])

axes[1, 0].plot([tax(c, n) for (c, n) in
                 zip(c_list[cap_t-pTminus:cap_t+pTplus],
                     n_list[cap_t-pTminus:cap_t+pTplus])],
                'k-', lw=2,  label='$\\tau_n$')
axes[1, 0].plot([1 - 1 / (m.R - 1) * (c2 / (m.beta * c1) - 1)
                 for (c1, c2) in
                 zip(c_list[cap_t-pTminus-1:cap_t+pTplus-1],
                     c_list[cap_t-pTminus:cap_t+pTplus])],
                'k--', lw=2,  label='$\\phi_k$')
axes[1, 0].legend(('$\\tau_n$', '$\\phi_k$'), loc=3, fontsize=14)
axes[1, 0].set_title('(c) Labor and capital income tax')
axes[1, 0].set_xticks([0, 5, 10, 15, 20])
axes[1, 0].set_xticklabels([0, 5, 'T', 15, 20])
axes[1, 0].set_xlabel('time')

axes[1, 1].plot(a[cap_t-pTminus:cap_t+pTplus], 'k-', lw=2, label='$a$')
axes[1, 1].plot([at - At for (At, at) in
                 zip(big_A[cap_t-pTminus:cap_t+pTplus],
                     a[cap_t-pTminus:])],
                'k--', lw=2,   label='$b$')
axes[1, 1].plot(big_A[cap_t-pTminus:cap_t+pTplus], 'k-.', lw=2,
                label='$A$')
axes[1, 1].legend(('$a$', '$b$', '$A$'), loc=1, fontsize=14)
axes[1, 1].set_title('(d) Assets and liabilities')
axes[1, 1].set_xticks([0, 5, 10, 15, 20])
axes[1, 1].set_xticklabels([0, 5, 'T', 15, 20])
axes[1, 1].set_xlabel('time')

axes[0, 1].plot([m.u(c, n) - udev for c, n in zip(
                    c_list[cap_t-pTminus:cap_t+pTplus],
                    n_list[cap_t-pTminus:cap_t+pTplus])])
axes[0, 1].set_title('(b) Utility relative to deviation utility')
axes[0, 1].set_xlabel('')
axes[0, 1].set_xticks([0, 5, 10, 15, 20])
axes[0, 1].set_xticklabels([0, 5, 'T', 15, 20])

plt.show()