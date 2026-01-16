import matplotlib.pyplot as plt
from matplotlib import animation
from matplotlib import colors
import numpy as np

ON = 255
OFF = 0
ON_NUM = ON


def add_glider(i, j, grid):
    glider = np.array([[OFF, OFF, ON],
                       [ON, OFF, ON],
                       [OFF, ON, ON]])
    grid[i:i + 3,
    j:j + 3] = glider  # slice specific to numpy arrays, first takes rows, second takes columns in taken rows


def init_grid_zeroes(N):
    grid = np.zeros(N * N).reshape(N, N)  #
    return grid


def init_grid_random(size=100, probabilities=(0.2, 0.8)):
    grid = np.random.choice([ON, OFF], size ** 2, p=probabilities).reshape(size, size)
    return grid


class AllAreDeadException(Exception):
    pass

class ColonyStagnationException(Exception):
    pass

def update(frame_num, img, grid: np.ndarray, N):
    print(frame_num)
    new_grid = grid.copy()
    n_living = 0
    n_changes = 0
    for x in range(N):
        for y in range(N):
            orig = grid[x][y]
            up = (y - 1) % N  # 0,0 is upper left corner
            down = (y + 1) % N
            right = (x + 1) % N
            left = (x - 1) % N
            sum = int(grid[right][y] + grid[left][y] + grid[x][up] + grid[x][down] +
                      grid[right][up] + grid[right][down] + grid[left][up] + grid[left][down])
            sum /= ON_NUM

            if sum < 2 or sum > 3:
                grid[x][y] = OFF
            if sum == 3:
                grid[x][y] = ON
                n_living += 1
            if orig != grid[x][y]: #value was changed
                n_changes += 1
    grid = new_grid[:]
    img.set_data(grid)
    if n_living == 0:
        raise AllAreDeadException
    if n_changes == 0:
        raise ColonyStagnationException
    return img


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Conway's Game of Life")
    parser.add_argument("--grid-size", default=100, type=int, required=False)
    parser.add_argument("--interval", dest='interval', default=500, type=int, required=False)
    parser.add_argument("--movfile", dest='movfile', default="", type=str, required=False)
    parser.add_argument("--glider", dest='glider', action="store_true", default=False, required=False)
    parser.add_argument("--interpolation", default='gaussian', type=str, required=False, help='one of neararest,bilinear,gaussian')
    parser.add_argument("--frames", default=None, required=False, help="If specified, sets number frames for movfile")

    args = parser.parse_args()
    if args.glider:
        grid = init_grid_zeroes(args.grid_size)
        add_glider(1, 1, grid)
    else:
        grid = init_grid_random(args.grid_size,(0.1,0.9))
    try:
        fig, ax = plt.subplots()
        cmap = colors.ListedColormap(['black', 'red']) # for black background, red cells
        #img = ax.imshow(grid, interpolation='nearest', cmap = cmap)
        #img = ax.imshow(grid, interpolation='bilinear', cmap=cmap)
        #img = ax.imshow(grid, interpolation='gaussian', cmap=cmap)
        img = ax.imshow(grid, interpolation=args.interpolation)
        frames = args.frames
        animation = animation.FuncAnimation(fig, update, fargs=(img, grid, args.grid_size), frames=frames,
                                            interval=args.interval)

        if args.movfile:
            animation.save(args.movfile, fps=30, extra_args=['-vcodec', 'libx264'])
            #animation.save(args.movfile, fps=30)

        plt.show()
    except AllAreDeadException:
        print("Game Over: all died")
    except ColonyStagnationException:
        print("Game Over, your colony is stagnating for lifetime")