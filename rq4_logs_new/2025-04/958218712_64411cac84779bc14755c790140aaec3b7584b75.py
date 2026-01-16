import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation

def animasjon(method,tittel, N, M, k, h):
    fig, ax = plt.subplots()
    ax.set_xlim(0, 2 * np.pi)
    ax.set_ylim(-1, 1)
    line, = ax.plot([], [], label="Metode")
    
    def init():
        line.set_data([], [])
        return line,

    def update(frame):
        x, u = method(N, frame, k, h)
        line.set_data(x, u)
        return line,

    ani = animation.FuncAnimation(fig, update, frames=range(1, M+1), init_func=init, blit=True, interval=100)
    plt.title(tittel)
    plt.show()
