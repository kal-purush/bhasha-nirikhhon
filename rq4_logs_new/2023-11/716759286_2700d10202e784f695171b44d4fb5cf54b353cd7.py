import matplotlib.pyplot as plt
import matplotlib.axes
import config

def get_sandbox_plot() -> matplotlib.axes.Axes:
    '''Matplotlib axes with objects defined in config

    Returns
    -------
    matplotlib.axes.Axes
        Sandbox
    '''
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.set_xlim(0, config.BOX_WIDTH)
    ax.set_ylim(0, config.BOX_LENGTH)

    for obs in config.OBSTACLES:
        ax.add_patch(obs.get_patch())

    for wp in config.WAY_POINTS:
        ax.scatter([wp[0]], [wp[1]], color='b')

    return ax

def main():
    ax = get_sandbox_plot()
    plt.show()


if __name__ == '__main__':
    main()