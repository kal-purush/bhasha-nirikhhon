import numpy as np
import math
import matplotlib as mp
import matplotlib.pyplot as plt


def getXcYc(x, k1m, k3, k3m, k2, n):
    k1 = [0.0] * n
    y = [0.0] * n
    z = [0.0] * n
    alk = math.sqrt(k3) / (math.sqrt(k3m) + math.sqrt(k3))
    for i in range(n):
        y[i] = (1 - x[i]) * alk
        z[i] = 1 - x[i] - y[i]
        k1[i] = (k1m*x[i]+k2*z[i]**2*x[i])/(z[i])
    figure, axes = plt.subplots()
    plt.xlabel("k1")
    plt.ylabel("x,y")
    axes.plot(k1, x, 'r')
    axes.plot(k1, y, 'b')
    plt.show()

if __name__ == "__main__":
    # двухпараметрический анализ на плоскости (k1,k1m)
    k2 = 1.05
    k3m = 0.002
    k3 = 0.0032
    alk = math.sqrt(k3) / (math.sqrt(k3m) + math.sqrt(k3))
    x = np.linspace(0, 0.999, 1000)
    n = len(x)
    y = [0.0]*1000
    z = [0.0]*1000
    k1 = [0.0]*1000
    k1m = [0.0]*1000
    sp = [0.0]*1000
    det = [0.0]*1000
    K1 = [0.0]*1000
    K1m = [0.0]*1000
    Sp = [0.0]*1000
    Det = [0.0]*1000

    # #зависимость от k1 для неск. значений параметра k1m
    # getXcYc(x, 0.001, k3, k3m, k2, n)
    # getXcYc(x, 0.005, k3, k3m, k2, n)
    # getXcYc(x, 0.01, k3, k3m, k2, n)
    # getXcYc(x, 0.015, k3, k3m, k2, n)
    # getXcYc(x, 0.02, k3, k3m, k2, n)
    #
    # # зависимость от k1 для неск. значений параметра k3m
    # getXcYc(x, 0.005, k3, 0.0005, k2, n)
    # getXcYc(x, 0.005, k3, 0.001, k2, n)
    # getXcYc(x, 0.005, k3, 0.002, k2, n)
    # getXcYc(x, 0.005, k3, 0.003, k2, n)
    # getXcYc(x, 0.005, k3, 0.004, k2, n)

    # расчет линии нейтральности
    for i in range(n):
        y[i] = (1 - x[i]) * alk
        z[i] = 1 - x[i] - y[i]
        k1m[i] = (-k2 * z[i] ** 2 * x[i] - k2 * z[i] ** 3 + 2 * k2 * x[i] * z[i] ** 2 - 2 * k3 * z[
            i] ** 2 - 2 * k3m * y[i] * z[i]) / (x[i] + z[i])
        k1[i] = (k1m[i] * x[i] + k2 * z[i] ** 2 * x[i]) / (z[i])
        a11 = -k1[i] - k1m[i] - k2 * z[i] ** 2 + 2 * k2 * x[i] * z[i]
        a12 = -k1[i] + 2 * k2 * x[i] * z[i]
        a21 = -2 * k3 * z[i]
        a22 = -2 * k3 * z[i] - 2 * k3m * y[i]
        det[i] = a11 * a22 - a12 * a21
        sp[i] = a11 + a22
        if det[i] <= 0:
            k1m[i] = -1

    # расчет линии кратности
    for i in range(n):
        K1m[i] = ((k2 * z[i] ** 2 * x[i] + k2 * z[i] ** 3 - 2 * k2 * x[i] * z[i]) * (
        2 * k3 * z[i] + 2 * k3m * y[i]) - (k2 * z[i] ** 2 * x[i] - 2 * k2 * x[i] * z[i] ** 2) * 2 * k3 * z[i]) / (
                   x[i] * 2 * k3 * z[i] - (x[i] + z[i]) * (2 * k3 * z[i] + 2 * k3m * y[i]))
        K1[i] = (K1m[i] * x[i] + k2 * z[i] ** 2 * x[i]) / (z[i])
        a11 = -K1[i] - K1m[i] - k2 * z[i] ** 2 + 2 * k2 * x[i] * z[i]
        a12 = -K1[i] + 2 * k2 * x[i] * z[i]
        a21 = -2 * k3 * z[i]
        a22 = -2 * k3 * z[i] - 2 * k3m * y[i]
        Det[i] = a11 * a22 - a12 * a21
        Sp[i] = a11 + a22

    # #построение фазового портрета
    # figure, axes = plt.subplots()
    # axes.set_title("Parametric Portrait")
    # plt.xlabel("km1")
    # plt.ylabel("k1")
    # axes.plot(k1m, k1, 'r')
    # axes.plot(K1m, K1, 'b')
    # plt.show()

    #однопараметрический анализ по параметру k1
    k1mi = 0.005
    i = 0
    DI = [0.0]*1000
    yh = []
    xh = []
    k1h = []
    ysn = []
    xsn = []
    k1sn = []
    ydi = []
    xdi = []
    k1di = []
    k1[i] = (k1mi * x[i] + k2 * z[i] ** 2 * x[i]) / (z[i])
    a11 = -k1[i] - k1mi - k2 * z[i] ** 2 + 2 * k2 * x[i] * z[i]
    a12 = -k1[i] + 2 * k2 * x[i] * z[i]
    a21 = -2 * k3 * z[i]
    a22 = -2 * k3 * z[i] - 2 * k3m * y[i]
    sp[i] = a11 + a22
    det[i] = a11*a22 - a12*a21
    DI[i] = sp[i]**2 - 4*det[i]
    for i in range(1, n):
        k1[i] = (k1mi * x[i] + k2 * z[i] ** 2 * x[i]) / (z[i])
        a11 = -k1[i] - k1mi - k2 * z[i] ** 2 + 2 * k2 * x[i] * z[i]
        a12 = -k1[i] + 2 * k2 * x[i] * z[i]
        a21 = -2 * k3 * z[i]
        a22 = -2 * k3 * z[i] - 2 * k3m * y[i]
        sp[i] = a11 + a22
        det[i] = a11 * a22 - a12 * a21
        DI[i] = sp[i] ** 2 - 4 * det[i]
        if sp[i]*sp[i-1]<=0:
            yh.append(y[i])
            xh.append(x[i])
            k1h.append(k1[i])
        if det[i]*det[i-1] <= 0:
            ysn.append(y[i])
            xsn.append(x[i])
            k1sn.append(k1[i])
        if DI[i]*DI[i-1] <= 0:
            ydi.append(y[i])
            xdi.append(x[i])
            k1di.append(k1[i])

    print(k1di)
    print(xdi)
    print(ydi)

    # figure, axes = plt.subplots()
    # axes.set_title("")
    # plt.xlabel("k1")
    # plt.ylabel("x,y")
    # axes.plot(k1h, xh, 'r')
    # axes.plot(k1h, yh, 'b')
    # plt.show()

    # figure, axes = plt.subplots()
    # axes.set_title("")
    # plt.xlabel("k1")
    # plt.ylabel("x,y")
    # axes.plot(k1sn, xsn, 'r')
    # axes.plot(k1sn, ysn, 'b')
    # plt.show()
