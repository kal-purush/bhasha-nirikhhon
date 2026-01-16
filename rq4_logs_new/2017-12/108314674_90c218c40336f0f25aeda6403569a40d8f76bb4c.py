from Pan_tompkins import stworz_wykres
import numpy as np
import matplotlib.pyplot as plt


def znajdz_index(lista, start, stop, wartosc):
    for i in range(stop - start):
        val = abs(lista[start + i])
        diff = abs(val - wartosc)
        if diff < 0.001:
            return i + start
    print('Dupa')
    return 0


def znajdz_granice(ECG, index):
    licznik, val, g1, g2 = 0, ECG[index], 0, 0
    poprzednia = ECG[index] + 1
    while poprzednia > val:
        poprzednia = val
        licznik -= 1
        val = ECG[index + licznik]
    g1 = index + licznik
    licznik, val = 0, ECG[index]
    poprzednia = val + 1
    while poprzednia > val:
        poprzednia = val
        licznik += 1
        val = ECG[index + licznik]
    g2 = index + licznik
    return g1, g2


def znajdz_p_t(nazwa_pliku):
    ind_p, ind_n, Time, ECG, q, r, s, ind_q, ind_r, ind_s = stworz_wykres(nazwa_pliku)
    licznik = min(len(ind_n), len(ind_p))
    t = np.zeros((2, licznik - 1))
    p = np.zeros((2, licznik - 1))
    p1,p2, t1, t2 = np.zeros(licznik - 1), np.zeros(licznik - 1), np.zeros(licznik - 1), np.zeros(licznik - 1)
    for i in range(licznik - 1):
        mean_np = int((ind_n[i] + ind_p[i + 1]) / 2)
        t_value = max(ECG[ind_n[i] : mean_np])
        t_index = znajdz_index(ECG, ind_n[i], mean_np, t_value)
        t[0, i] = Time[t_index]
        t[1, i] = t_value
        g1, g2 = znajdz_granice(ECG, t_index)
        t1[i] = Time[g1]
        t2[i] = Time[g2]
        p_value = max(ECG[mean_np : ind_p[i + 1]])
        p_index = znajdz_index(ECG, mean_np, ind_p[i + 1], p_value)
        p[0, i] = Time[p_index]
        p[1, i] = p_value
        g1, g2 = znajdz_granice(ECG, p_index)
        p1[i] = Time[g1]
        p2[i] = ECG[g1]
    pp = plt.plot(Time, ECG, '-', r[0], r[1], 'ro', q[0], q[1], '^y', s[0], s[1], 'ks', p[0], p[1], 'bo', t[0], t[1], '^g')
    plt.xlim([10, 15])
    plt.show()
    return p1, p2, t1, t2
