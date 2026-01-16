#!/usr/bin/env python -u

# Produce a pair of composite plots of the clean experiment

# Author James Dempsey 20 Nov 2016

from __future__ import print_function, division

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import glob


trial_path = '/Users/jamesdempsey/Documents/RSAA/magmo/prev_runs/20161023'


def plot_source(source, filename):
    suffixes = ['baseclean', '2000pos', 'ni1000', 'ni500', 'negstop',
                'cutoff3-250', 'cutoff3-250-pos', 'ni250', 'sdi250']
    titles = ['2000 Iterations', '2000 Iterations, Positive', '1000 Iterations',
              '500 Iterations', '2000 Iterations, Negstop',
              '250 Iterations, 3*RMS Cutoff',
              '250 Iterations, 3*RMS Cutoff, Positive', '250 Iterations',
              'SDI, 250 Iterations']

    fig_size = plt.rcParams["figure.figsize"]
    print (fig_size)
    fig_size[0] = 24
    fig_size[1] = 15
    #plt.rcParams["figure.figsize"] = fig_size

    i = 0
    for suffix in suffixes:
        img_path = trial_path + '/day21-' + suffix + '/' + source + '_plot.png'
        img = mpimg.imread(img_path)
        plt.subplot(3, 3, i+1)
        imgplot = plt.imshow(img)
        plt.title(titles[i])
        plt.xticks(()), plt.yticks(())
        i += 1

    plt.savefig(filename)
    plt.close()


def main():
    sources = ['285.337-0.002_src5-0', '291.270-0.719_src6-5']
    i = 0
    for src in sources:
        i += 1
        plot_source(src, "clean_plot_" + str(i) + ".pdf")
    return 0


if __name__ == '__main__':
    exit(main())