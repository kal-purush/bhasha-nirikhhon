import numpy as np
import matplotlib.pyplot as plt


def i1(u, f, dx):
    xs = np.arange(0, 1, dx)
    return np.trapz(np.exp(-u(xs) + f * xs), dx=dx)


def i2(x, u, f, dx):
    if x == 0.0:
        return 0.0
    xs = np.arange(0, x, dx)
    return np.trapz(np.exp(u(xs) - f*xs), dx=dx)


def i3(u, f, dx):
    xs = np.arange(0, 1, dx)
    s2s = np.vectorize(i2)(xs, u, f, dx)
    return np.trapz(np.exp(-u(xs) + f * xs) * s2s, dx=dx)


def l2(x, u, f, dx):
    if x == 0.0:
        return 0.0
    xs = np.arange(x, 1, dx)
    return np.trapz(np.exp(u(xs) - f*xs), dx=dx)


def l3(u, f, dx):
    xs = np.arange(0, 1, dx)
    s2s = np.vectorize(l2)(xs, u, f, dx)
    return np.trapz(np.exp(-u(xs) + f * xs) * s2s, dx=dx)


def u1(x):
    beta = 1.0
    return - u_hb * ((1.0 - np.cos(2*np.pi*x)) / 2)**beta


def u2(x):
    beta = 2.0
    return - u_hb * ((1.0 - np.cos(2*np.pi*x)) / 2)**beta


def u10(x):
    beta = 10.0
    return - u_hb * ((1.0 - np.cos(2*np.pi*x)) / 2)**beta


def u05(x):
    beta = 0.5
    return - u_hb * ((1.0 - np.cos(2*np.pi*x)) / 2)**beta


def u01(x):
    beta = 0.1
    return - u_hb * ((1.0 - np.cos(2*np.pi*x)) / 2)**beta


def s(fs, u, dx):
    s_ = []
    for f in fs:
        if f < u_hb:
            a = i1(u, f, dx)
            c = i2(1.0, u, f, dx)
            b = i3(u, f, dx)

            s_.append((a * c) / (1.0 - np.exp(-f)) - b)
        else:
            b = l3(u, f, dx)
            s_.append(b)
    return np.array(s_)


def s_f(fs, u, dx):
    s_ = []
    for f in fs:
        b = l3(u, f, dx)
        s_.append(b)
    return np.array(s_)


def log_scaling(v):
    v0 = 2 * np.pi * u_hb * np.exp(-u_hb)
    print('v0=%s' % str(v0))

    return 2 * np.log(v)


def lin_scaling(v):
    a = np.exp(u_hb) / np.pi / u_hb

    print(a)

    return v * a

dx = 0.001

u_hb  = 30.0
f_max = 300.0
v0 = 2 * np.pi * u_hb * np.exp(-u_hb)
print('v0=%s' % str(v0))


num = 1000
f = np.geomspace(f_max/num, f_max, num)
# v01 = 1 / s(f, u01, dx) / v0
# v05 = 1 / s(f, u05, dx) / v0
s_ = s(f, u1, dx)
# s__ = s_f(f, u1, dx)
vf = 1.0 / s_ / v0
# v1 = 1.0 / s(f, u1, dx) / v0
# v2 = 1 / s(f, u2, dx) / v0
# v10 = 1 / s(f, u10, dx) / v0

# plt.plot(f, u(f))
# plt.show()


plt.loglog(f, f * s_, label='foo')
# plt.loglog(f, f * s__, label='f >> 1')
plt.legend()
plt.show()

# plt.loglog(v1, f, label='f >> 1')
# plt.loglog(v2, f, label='solution $\\beta=$1')
# plt.legend()
# plt.show()

# vs = v1
# plt.loglog(vs, log_scaling(vs), label='log')
# plt.loglog(v1, lin_scaling(v1 * v0), label='lin')
# plt.loglog(v1, f, label='solution $\\beta=$1')
# plt.loglog(vf, f, label='f >> 1')
# plt.xlabel('$v/v_0$')
# plt.ylabel('$F/F_0$')
# plt.legend()
# plt.show()


# x = np.arange(-1, 1, 0.01)
# plt.plot(x, u01(x) / u_hb, label='U $\\beta=$0.1')
# plt.plot(x, u05(x) / u_hb, label='U $\\beta=$0.5')
# plt.plot(x, u1(x) / u_hb, label='U $\\beta=$1')
# plt.plot(x, u2(x) / u_hb, label='U $\\beta=$2')
# plt.plot(x, u10(x) / u_hb, label='U $\\beta=$10')
# plt.xlabel('$x/a$')
# plt.ylabel('$U/U_{HB}$')
# plt.legend()
# plt.show()
#
# plt.loglog(v01, f, label='solution $\\beta=$0.1')
# plt.loglog(v05, f, label='solution $\\beta=$0.5')
# plt.loglog(v1, f, label='solution $\\beta=$1')
# plt.loglog(v2, f, label='solution $\\beta=$2')
# plt.loglog(v10, f, label='solution $\\beta=$10')
# plt.xlabel('$v/v_0$')
# plt.ylabel('$F/F_0$')
# plt.legend()
# plt.show()
#
# plt.loglog(f, f/v01, label='solution $\\beta=$0.1')
# plt.loglog(f, f/v05, label='solution $\\beta=$0.5')
# plt.loglog(f, f/v1, label='solution $\\beta=$1')
# plt.loglog(f, f/v2, label='solution $\\beta=$2')
# plt.loglog(f, f/v10, label='solution $\\beta=$10')
# plt.xlabel('$F/F_0$')
# plt.ylabel('$(F/F_0) / (v/V_0)$')
# plt.legend()
# plt.show()