#!/usr/bin/env python
###############################################################################

import argparse, os, sys

from datetime import datetime
from matplotlib import pyplot
from matplotlib import pylab

###############################################################################
###############################################################################

def histogram(arguments):

    data = list(map(float, sys.stdin.readlines()))
    data_min = min(data)
    data_avg = pylab.average(pylab.array(data))
    data_max = max(data)
    data_std = pylab.std(pylab.array(data))

    data = filter(
        lambda n: data_avg + arguments.n * data_std > (n**2)**0.5, data)

    pyplot.hist(list(data), bins=arguments.bins)
    pyplot.suptitle(arguments.suptitle)

    if arguments.title is None:
        pyplot.title('min|avg|max|std = {0:0.2f}|{1:0.2f}|{2:0.2f}|{3:0.2f}'
            .format(data_min, data_avg, data_max, data_std))
    else:
        pyplot.title(arguments.title)

    pyplot.xlabel(arguments.xlabel)
    pyplot.ylabel(arguments.ylabel)
    pyplot.grid()

    pyplot.savefig(path(arguments))

def path(arguments):
    ts = timestamp()
    ext = arguments.image_ext
    return arguments.image_tpl.format(ts, ext)

def timestamp(value=None):
    if value is None: value = datetime.now()
    return str(value).replace(' ', 'T')[:-3] + 'Z'

###############################################################################
###############################################################################

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='Plotter',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers()

    parser.add_argument('--title', type=str,
        default=os.environ.get('PLOT_TITLE', None),
        help='Plot title')
    parser.add_argument('--suptitle', type=str,
        default=os.environ.get('PLOT_SUPTITLE', None),
        help='Plot sup-title')
    parser.add_argument('--xlabel', type=str,
        default=os.environ.get('PLOT_XLABEL', 'Bins'),
        help='Plot x-label')
    parser.add_argument('--ylabel', type=str,
        default=os.environ.get('PLOT_YLABEL', 'Frequencey'),
        help='Plot y-label')

    parser.add_argument('-n', type=float,
        default=os.environ.get('PLOT_N', 5.0),
        help='Plot standard deviation cap')

    parser.add_argument('--image-tpl', type=str,
        default=os.environ.get('PLOT_IMAGE_TPL', 'img-[{0}].{1}'),
        help='Output image template')
    parser.add_argument('--image-ext', type=str,
        default=os.environ.get('PLOT_IMAGE_EXT', 'png'),
        help='Output image extension')

    parser_plot = subparsers.add_parser('histogram',
        help='Plots a histogram')
    parser_plot.add_argument('-n', '--bins', type=int,
        default=os.environ.get('HISTOGRAM_BINS', 10),
        help='Number of bins')
    parser_plot.set_defaults(func=histogram)

    parser.add_argument('-v', '--version', action='version',
        version='%(prog)s 0.0.1')

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)

###############################################################################
###############################################################################