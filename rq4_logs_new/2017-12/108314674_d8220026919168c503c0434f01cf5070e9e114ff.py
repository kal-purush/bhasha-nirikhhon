# WYZNACZANIE PARAMETRÓW SYGNAŁU EKG

import matplotlib.pyplot as plt
import numpy as np

from Pan_tompkins import stworz_wykres
from svm import p1, p2, t1, t2
from Load_file import get_values_from_file


def basic_parameters(ECG):
    ppValue = max(ECG) - min(ECG)  # amplituda sygnału - wartość międzyszczytowa
    meanValue = np.mean(ECG)  # wartość średnia sygnału
    return ppValue, meanValue


# odległość pomiędzy załamkami R (odległość R-R)
def RR_interval(R):
    N = len(R[0])
    # utworzenie tablicy dwuwymiarowej:
    # pierwszy wymiar - wektor położenia na osi czasu punktów początków kolejnych odległości R-R
    # drugi wymiar - wektor wartości odległości pomiędzy każdymi kolejnymi dwoma załamkami R
    RR = np.zeros((2, N - 1))
    for i in range(N - 1):
        RR[0][i] = R[0][i]  # wartości w [s]
        RR[1][i] = (R[0][i + 1] - R[0][i]) * 1000  # wartości w [ms]
    return RR


# odległości pomiędzy początkami załamków P (odległości P-P)
def PP_interval(P):
    N = len(P[0])
    # tablica dwuwymiarowa:
    # pierwszy wymiar - wektor położenia na osi czasu punktów początków kolejnych odległości P-P
    # drugi wymiar - wektor wartości odległości pomiędzy każdymi kolejnymi dwoma początkami załamków P
    PP = np.zeros((2, N - 1))
    for i in range(N - 1):
        PP[0][i] = P[0][i]  # wartości w [s]
        PP[1][i] = (P[0][i + 1] - P[0][i]) * 1000  # wartości w [ms]
    return PP


# tętno (HR - heart rate)
def heart_rate(R):
    N = len(R[0])
    RR = RR_interval(R)  # wygenerowanie wektora odległości R-R
    # utworzenie tablicy dwuwymiarowej:
    # pierwszy wymiar - wektor położenia na osi czasu punktów początków kolejnych odległości R-R
    # drugi wymiar - wektor wartości tętna
    HR = np.zeros((2, N - 1))  # wektor tętna mierzonego jako odwrotność odległości R-R
    for i in range(N - 1):
        HR[0][i] = R[0][i]  # wartości w [s]
        HR[1][i] = (1 / RR[1][i]) * 60 * 1000  # wartości w [bpm] (beats per minute)
    return HR


# czas trwania załamka P
def P_wave(P):
    N = len(P[0])
    # tablica dwuwymiarowa:
    # pierwszy wymiar - wektor położenia na osi czasu punktów początków kolejnych załamków P
    # drugi wymiar - wektor wartości długości kolejnych załaów P
    P_len = np.zeros((2, N))
    for i in range(N):
        P_len[0][i] = P[0][i]  # wartości w [s]
        P_len[1][i] = (P[1][i] - P[0][i]) * 1000  # wartości w [ms]
    return P_len


# czas trwania zespołu QRS
def QRS_duration(Q, S):
    N = len(Q[0])
    # tablica dwuwymiarowa:
    # pierwszy wymiar - wektor położenia na osi czasu punktów początków kolejnych zespołów QRS
    # drugi wymiar - wektor wartości długości kolejnych zespołów QRS
    QRS_dur = np.zeros((2, N))
    for i in range(N):
        QRS_dur[0][i] = Q[0][i]  # wartości w [s]
        QRS_dur[1][i] = (S[0][i] - Q[0][i]) * 1000  # wartości w [ms]
    return QRS_dur


# czas trwania załamka T
def T_wave(T):
    N = len(T[0])
    # tablica dwuwymiarowa:
    # pierwszy wymiar - wektor położenia na osi czasu punktów początków kolejnych załamków T
    # drugi wymiar - wektor wartości długości kolejnych załaów T
    T_len = np.zeros((2, N))
    for i in range(N):
        T_len[0][i] = T[0][i]  # wartości w [s]
        T_len[1][i] = (T[1][i] - T[0][i]) * 1000  # wartości w [ms]
    return T_len


# wartości średnie wyznaczonych parametrów sygnału
def mean_values(P, Q, R, S, T):
    # utworzenie słownika z wartościami średnimi parametrów
    mean_val = {'mean_RR_interval': np.mean(RR_interval(R)[1]), 'mean_PP_interval': np.mean(PP_interval(P)[1]),
                'mean_heart_rate': np.mean(heart_rate(R)[1]), 'mean_P_wave': np.mean(P_wave(P)[1]),
                'mean_QRS_duration': np.mean(QRS_duration(Q, S)[1]), 'mean_T_wave': np.mean(T_wave(T)[1])}
    return mean_val

# funkcja zwracająca wartości parametrów
def oblicz_parametry(nazwa_pliku):
    [Time, Resp, BPL, ECG] = get_values_from_file(nazwa_pliku)
    [ind_p, ind_n, Time, Q, R, S] = stworz_wykres(nazwa_pliku)
    P = [p1[0], p2[0]]  # położenie na osi czasu: pierwszy wymiar - początki fal P, drugi wymiar - końce fal P
    T = [t1[0], t2[0]]  # położenie na osi czasu: pierwszy wymiar - początki fal T, drugi wymiar - końce fal T

    ppValue, meanValue = basic_parameters(ECG)
    RR = RR_interval(R)
    PP = PP_interval(P)
    HR = heart_rate(R)
    P_len = P_wave(P)
    QRS_dur = QRS_duration(Q, S)
    T_len = T_wave(T)
    mean_val = mean_values(P, Q, R, S, T)

    return ppValue, meanValue, RR, PP, HR, P_len, QRS_dur, T_len, mean_val

# funkcja rysująca wykresy
def wykresy_parametrow(nazwa_pliku):
    [ind_p, ind_n, Time, Q, R, S] = stworz_wykres(nazwa_pliku)
    P = [p1[0], p2[0]]  # położenie na osi czasu: pierwszy wymiar - początki fal P, drugi wymiar - końce fal P
    T = [t1[0], t2[0]]  # położenie na osi czasu: pierwszy wymiar - początki fal T, drugi wymiar - końce fal T

    # wykresy: tętno, R-R, P-P
    fig1, (ax1, ax2, ax3) = plt.subplots(3, 1)
    fig1.subplots_adjust(hspace=0.5)
    ax1.scatter(heart_rate(R)[0], heart_rate(R)[1])
    ax1.set_xlabel('czas [s]')
    ax1.set_ylabel('Tętno [bpm]')
    ax1.set_xlim([0, heart_rate(R)[0][-1] + 0.5])
    ax1.set_ylim([50, 100])
    ax1.plot(heart_rate(R)[0], [60] * len(heart_rate(R)[0]), '-r')
    ax1.plot(heart_rate(R)[0], [80] * len(heart_rate(R)[0]), '-r')

    ax2.scatter(RR_interval(R)[0], RR_interval(R)[1])
    ax2.set_xlabel('czas [s]')
    ax2.set_ylabel('Odległość R-R [ms]')
    ax2.set_xlim([0, RR_interval(R)[0][-1] + 0.5])
    ax2.set_ylim([400, 1600])
    ax2.plot(RR_interval(R)[0], [600] * len(RR_interval(R)[0]), '-r')
    ax2.plot(RR_interval(R)[0], [1200] * len(RR_interval(R)[0]), '-r')

    ax3.scatter(PP_interval(P)[0], PP_interval(P)[1])
    ax3.set_xlabel('czas [s]')
    ax3.set_ylabel('Odległość P-P [ms]')
    ax3.set_xlim([0, PP_interval(P)[0][-1] + 0.5])
    ax3.set_ylim([400, 1600])
    ax3.plot(PP_interval(R)[0], [600] * len(PP_interval(R)[0]), '-r')
    ax3.plot(PP_interval(R)[0], [1200] * len(RR_interval(R)[0]), '-r')
    plt.show()

    # wykresy: P, QRS, T
    fig2, (ax1, ax2, ax3) = plt.subplots(3, 1)
    fig2.subplots_adjust(hspace=0.5)
    ax1.scatter(P_wave(P)[0], P_wave(P)[1])
    ax1.set_xlabel('czas [s]')
    ax1.set_ylabel('szerokość P [ms]')
    ax1.set_xlim([0, P_wave(P)[0][-1] + 0.5])
    ax1.set_ylim([0, 140])
    ax1.plot(P_wave(P)[0], [120] * len(P_wave(P)[0]), '-r')

    ax2.scatter(QRS_duration(Q, S)[0], QRS_duration(Q, S)[1])
    ax2.set_xlabel('czas [s]')
    ax2.set_ylabel('szerokość QRS [ms]')
    ax2.set_xlim([0, QRS_duration(Q, S)[0][-1] + 0.5])
    ax2.set_ylim([0, 200])
    ax2.plot(QRS_duration(Q, S)[0], [60] * len(QRS_duration(Q, S)[0]), '-r')
    ax2.plot(QRS_duration(Q, S)[0], [120] * len(QRS_duration(Q, S)[0]), '-r')

    ax3.scatter(T_wave(T)[0], T_wave(T)[1])
    ax3.set_xlabel('czas [s]')
    ax3.set_ylabel('szerokość T [ms]')
    ax3.set_xlim([0, T_wave(T)[0][-1] + 0.5])
    ax3.set_ylim([60, 300])
    ax3.plot(T_wave(T)[0], [100] * len(T_wave(T)[0]), '-r')
    ax3.plot(T_wave(T)[0], [250] * len(T_wave(T)[0]), '-r')
    plt.show()