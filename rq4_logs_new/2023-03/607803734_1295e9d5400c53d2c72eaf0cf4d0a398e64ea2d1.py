from sampling import get_samples, get_gauss, get_rect
from scipy.fft import fft, ifft
import matplotlib.pyplot as plt


def convolution(signal_1, signal_2, N):
    U = [0 for i in range(2 * N)]
    H = [0 for i in range(2 * N)]
    for n in range(N):
        U[n] = signal_1[n]
        H[n] = signal_2[n]
    v = fft(U)
    h = fft(H)
    w = [v[k] * h[k] for k in range(2 * N)]
    return ifft(w)


if __name__ == '__main__':
    sigma = 0.5  # для сигнала Гаусса
    tt = 4  # для прямоугольного импульса
    samples_num = 256
    dt = 0.05
    t_max = dt * (samples_num - 1) / 2  # правая граница
    samples = get_samples(t_max, dt)  # набор отсчетов
    # сигналы
    gauss_1 = get_gauss(samples, sigma)
    gauss_2 = get_gauss(samples, sigma * 2)
    rect_1 = get_rect(samples, tt)
    rect_2 = get_rect(samples, tt / 2)

    rect_rect = convolution(rect_1, rect_2, samples_num)
    gauss_gauss = convolution(gauss_1, gauss_2, samples_num)
    rect_gauss = convolution(rect_1, gauss_1, samples_num)

    n_graph = [i for i in range(2 * samples_num)]

    plt.title('Исходные сигналы Гаусса')
    plt.xlabel('x')
    plt.ylabel('U(x)')
    plt.plot(samples, gauss_1, label='σ = 0.5')
    plt.plot(samples, gauss_2, label='σ = 1')
    plt.legend()
    plt.grid()
    plt.show()

    plt.title('Исходные прямоугольные импульсы')
    plt.xlabel('x')
    plt.ylabel('U(x)')
    plt.plot(samples, rect_1, label='T = 2')
    plt.plot(samples, rect_2, label='T = 1')
    plt.legend()
    plt.grid()
    plt.show()

    plt.title('Результаты сверток')
    plt.xlabel('n')
    plt.ylabel('W(n)')
    plt.plot(n_graph, rect_rect, label='П+П')
    plt.plot(n_graph, gauss_gauss, label='Г+Г')
    plt.plot(n_graph, rect_gauss, label='П+Г')
    plt.legend()
    plt.grid()
    plt.show()