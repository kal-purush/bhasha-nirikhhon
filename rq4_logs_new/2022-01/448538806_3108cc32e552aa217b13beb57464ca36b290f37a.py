import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

from matplotlib.pyplot import cm

def make_colormap(seq):
    """Return a LinearSegmentedColormap
    seq: a sequence of floats and RGB-tuples. The floats should be increasing
    and in the interval (0,1).
    """
    seq = [(None,) * 3, 0.0] + list(seq) + [1.0, (None,) * 3]
    cdict = {'red': [], 'green': [], 'blue': []}
    for i, item in enumerate(seq):
        if isinstance(item, float):
            r1, g1, b1 = seq[i - 1]
            r2, g2, b2 = seq[i + 1]
            cdict['red'].append([item, r1, r2])
            cdict['green'].append([item, g1, g2])
            cdict['blue'].append([item, b1, b2])

    return mcolors.LinearSegmentedColormap('CustomMap', cdict)

def color_map_to_use(N):
    x = np.arange(N).astype(float)
    c = mcolors.ColorConverter().to_rgb
    rvb = make_colormap(
    [c('red'), 0.125, c('red'), c('orange'), 0.25, c('orange'),c('green'),0.5, c('green'),0.7, c('green'), c('blue'), 0.75, c('blue')])
    #color = rvb(x/N)
    color = cm.seismic(np.linspace(0,1,N))
    return color
