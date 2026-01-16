# line_maker.py

def make_line():
    x1, y1 = (1, 2)
    x2, y2 = (3, 4)
    x = 6.1
    slope = calc_slope(x1, y1, x2, y2)
    intercept = calc_intercept(x1, y1, slope)
    y = calc_y(x, slope, intercept)
    print(y)


def calc_slope(x1, y1, x2, y2):
    slope_val = (y2 - y1) / (x2 - x1)
    return slope_val


def calc_intercept(x1, y1, slope):
    intercept_val = y1 - slope * x1
    return intercept_val


def calc_y(x, slope, intercept):
    y_val = slope*x + intercept
    return y_val


if __name__ == "__main__":
    make_line()