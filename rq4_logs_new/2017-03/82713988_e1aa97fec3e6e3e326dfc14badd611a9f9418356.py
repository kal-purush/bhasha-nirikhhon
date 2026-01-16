import matplotlib.pyplot as plt
import numpy as np
import csv
import matplotlib.ticker as ticker
from os import listdir
from os.path import isfile, join
import scipy.signal
print(scipy.__version__)
x1 = np.linspace(0, 20, 20)
x2 = np.linspace(1, 21, 20)
y1 = x1 * x1
y2 = x2

data_path = "/Users/ming/Downloads/"
file_name = "run_.,tag_train-model-accuracy-accuracy.csv"


def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    r"""Smooth (and optionally differentiate) data with a Savitzky-Golay filter.
    The Savitzky-Golay filter removes high frequency noise from data.
    It has the advantage of preserving the original shape and
    features of the signal better than other types of filtering
    approaches, such as moving averages techniques.
    Parameters
    ----------
    y : array_like, shape (N,)
        the values of the time history of the signal.
    window_size : int
        the length of the window. Must be an odd integer number.
    order : int
        the order of the polynomial used in the filtering.
        Must be less then `window_size` - 1.
    deriv: int
        the order of the derivative to compute (default = 0 means only smoothing)
    Returns
    -------
    ys : ndarray, shape (N)
        the smoothed signal (or it's n-th derivative).
    Notes
    -----
    The Savitzky-Golay is a type of low-pass filter, particularly
    suited for smoothing noisy data. The main idea behind this
    approach is to make for each point a least-square fit with a
    polynomial of high order over a odd-sized window centered at
    the point.
    Examples
    --------
    t = np.linspace(-4, 4, 500)
    y = np.exp( -t**2 ) + np.random.normal(0, 0.05, t.shape)
    ysg = savitzky_golay(y, window_size=31, order=4)
    import matplotlib.pyplot as plt
    plt.plot(t, y, label='Noisy signal')
    plt.plot(t, np.exp(-t**2), 'k', lw=1.5, label='Original signal')
    plt.plot(t, ysg, 'r', label='Filtered signal')
    plt.legend()
    plt.show()
    References
    ----------
    .. [1] A. Savitzky, M. J. E. Golay, Smoothing and Differentiation of
       Data by Simplified Least Squares Procedures. Analytical
       Chemistry, 1964, 36 (8), pp 1627-1639.
    .. [2] Numerical Recipes 3rd Edition: The Art of Scientific Computing
       W.H. Press, S.A. Teukolsky, W.T. Vetterling, B.P. Flannery
       Cambridge University Press ISBN-13: 9780521880688
    """
    import numpy as np
    from math import factorial

    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve( m[::-1], y, mode='valid')


def read_record(file_name):
    """
    read a csv file and return each column in a dict
    :param file_name: the file to be open
    :return:
    """
    ret_dict = {
        "Step": [],
        "Value": []
    }
    with open(file_name) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            ret_dict["Step"].append(eval(row["Step"]))
            ret_dict["Value"].append(eval(row["Value"]))
    return ret_dict


def add_to_list(list, record, name, color):
    item = {
        "Step": record["Step"],
        "Value": record["Value"],
        "Name": name,
        "Color": color
    }
    list.append(item)


def smoothing_record(record, n=31, order=0):
    record["Value"] = savitzky_golay(np.asarray(record["Value"]), n, order)
    record["Step"] = record["Step"][0:len(record["Value"])]


def load_dir_to_list(dir):
    files = [f for f in listdir(dir) if isfile(join(dir, f)) and f.endswith(".csv")]
    ret_list = []
    for f in files:
        record = read_record(join(dir, f))
        smoothing_record(record)
        add_to_list(ret_list, record, f.split('.')[0], color=None)
    return ret_list


def plot_multi_line(line_list, title):
    figure = plt.figure()
    assert line_list != []
    for line in line_list:
        plt.plot(line["Step"], line["Value"], linewidth=1.5, label=line["Name"])

    axes = figure.get_axes()
    ax = axes[0]
    ax.yaxis.set_major_locator(ticker.MultipleLocator(0.1))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(0.02))
    ax.grid(linewidth=1.5)
    ax.grid(b=True, which='minor',  linestyle='--', linewidth=1)
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    plt.xlabel("steps")
    plt.ylabel("value")
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.title(title)
    plt.show()

line_list = load_dir_to_list("/Users/ming/projects/Deep_protein/cvs_dir")
plot_multi_line(line_list, "Accuracy")
