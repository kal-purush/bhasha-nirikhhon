#!/usr/bin/env python
#
# Ben Jones bj6@princeton.edu
# Princeton Summer 2015
#
# graphing.py: library with utils for graphing

import math
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os
import tempfile


MARKERS = ['-', '--']
# COLORS = [(0,0,0), (230,159,0), (86,180,233), (0,158,115), (240,228,66),
#           (0,114,178), (213,94,0), (204, 121, 167)]
# COLORS = ["#a6cee3", "#1f78b4", "#b2df8a", "#33a02c"]
COLORS = ["#1b9e77", "#d95f02", "#7570b3"]


def create_cdf(data, x_label, y_label, title=None, filename=None, error=None,
               log=False, ylog=False, invert=False, xlims=None, ylims=None,
               legend_loc=None):
    """Given a set of data, create a CDF of the data, optionally in log
    format

    """
    plt.close('all')
    if (data is None) or (len(data) == 0):
        return
    fig, ax = plt.subplots()
    # if we get a dict, then plot each of the given lines as a
    # different CDF on the same graph
    if type(data) == dict:
        labels = sorted(data.keys())
        for lab_idx in range(len(labels)):
            plt_label = labels[lab_idx]
            error_bar = None
            to_plot = data[plt_label]
            marker = MARKERS[lab_idx % len(MARKERS)]
            color = COLORS[lab_idx % len(COLORS)]
            if error is not None and plt_label in error:
                to_plot = zip(to_plot, error[plt_label])
                to_plot.sort(key=lambda x: x[0])
                to_plot, error_bar = zip(*to_plot)
            else:
                to_plot.sort()

            if invert:
                y_data = np.linspace(1, 0, len(to_plot))
            else:
                y_data = np.linspace(0, 1, len(to_plot))
            if error_bar is not None:
                if marker == "--":
                    plt.errorbar(to_plot, y_data, marker, xerr=error_bar,
                                 dashes=(5, 2), label=plt_label, color=color)
                else:
                    plt.errorbar(to_plot, y_data, marker, xerr=error_bar,
                                 label=plt_label, color=color)
            else:
                if marker == "--":
                    plt.plot(to_plot, y_data, marker, label=plt_label,
                             dashes=(5, 2), color=color)
                else:
                    plt.plot(to_plot, y_data, marker, label=plt_label,
                             color=color)
        plt.legend(loc='lower right')

    else:
        error_bar = None
        if error is not None:
            data = zip(data, error)
            data.sort(key=lambda x: x[0])
            data, error_bar = zip(*data)
        else:
            # data = data.order()
            data.sort()

        # create the y data for the CDF
        if invert:
            y_data = np.linspace(1, 0, len(data))
        else:
            y_data = np.linspace(0, 1, len(data))
        if error_bar is not None:
            plt.errorbar(data, y_data, xerr=error_bar)
        else:
            plt.plot(data, y_data)

    plt.grid()
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.ylim(-0.05, 1.05)
    if xlims is not None:
        plt.xlim(xlims)
    if ylims is not None:
        plt.ylim(ylims)
    if invert:
        ax.invert_xaxis()

    if log:
        plt.xscale('log')
    if ylog:
        plt.yscale('log')

    if legend_loc is not None:
        plt.legend(loc=legend_loc)

    if title is not None:
        plt.title(title)
    # plt.tight_layout()
    if filename is not None:
        plt.savefig(filename, fmt='pdf')
    else:
        plt.show()


def cdf(data, filename=None, error=None, log=False, invert=False, xlims=None):
    create_cdf(data, "", "", "", filename=filename, error=error, log=log,
               invert=invert, xlims=xlims)


def create_time_plot(data, times, ylabel, title=None, filename=None,
                     xlims=None, ylims=None, log=False):
    plt.close('all')
    fig, ax = plt.subplots()
    if type(data) == dict:
        for lab in data.keys():
            plt.plot(times[lab], data[lab], 'x', label=lab)
        # plt.legend(loc='upper left')
        plt.legend()
    else:
        plt.plot(times, data, 'x')

    plt.grid()
    plt.xlabel("Received Time")
    plt.ylabel(ylabel)
    if title is not None:
        plt.title(title)
    fig.autofmt_xdate()
    if xlims is not None:
        plt.xlim(xlims)
    if ylims is not None:
        plt.ylim(ylims)
    if log:
        plt.yscale('log')
    if filename is not None:
        plt.tight_layout()
        plt.savefig(filename, fmt='pdf')
    else:
        plt.show()


def time_plot(data, times, filename=None, xlims=None, ylims=None, log=False):
    create_time_plot(data, times, "", "", filename=filename, xlims=xlims,
                     ylims=ylims, log=log)


def create_world_map(data, title, normalize=False, parts=[]):
    """Create a URL to a colorized world map with the google maps API

    Params:
    normalize- scale the input so that all the fractions for plotting
        fall between 0 and 1. This is useful if your input is not a
        fraction between 0 and 1

    Note: you need to open this in a browser to plot

    Note: we expect the data as dictionary where the keys are two
    letter country codes and the values are numbers that we should
    plot with the heat map

    """
    # convert the dict to a list of tuples with country code and value
    outputs = data.items()
    # sort by value
    outputs = sorted(outputs, key=lambda entry: entry[1])
    min_v, max_v = float(outputs[0][1]), float(outputs[-1][1])
    print min_v, max_v
    if not normalize:
        min_v, max_v = 0., 1.
    if parts != []:
        min_v, max_v = parts

    # partition the space depending on the number of colors
    buckets = np.array([int(float(x[1] - min_v)/max_v * 5) for x in outputs])
    outputs = zip(buckets, *zip(*outputs))
    # parts = np.linspace(min_v * 100., (max_v - min_v) * 100. / max_v, 5)
    parts = np.linspace(min_v * 100., max_v * 100., num=6)
    labels = []
    for index in range(1, 6):
        # num1 = int(parts[index - 1])
        # num2 = int(parts[index])
        labels.append("{:.1f}-{:.1f}%".format(parts[index - 1],
                                              parts[index]))

    # define our color map (10 colors at most)
    # colors = ["fff7ec", "fee8c8", "fdd49e", "fdbb84", "fc8d59",
    #           "ef6548", "d7301f", "b30000", "7f0000", "000000"]
    #
    # use this map because it is colorblind friendly and photocopy
    # safe (it should still print in grayscale)
    # colors = ["ffffcc", "a1dab4", "41b6c4", "2c7fb8", "253494"]
    # this colorscheme should also print in grayscale
    # colors = ["fef0d9", "fdcc8a", "fc8d59", "d7301f"]
    # colors = ["c6dbef", "9ecae1", "6baed6", "3182bd", "08519c", "eff3ff"]
    colors = ["c6dbef", "9ecae1", "6baed6", "3182bd", "08519c"]

    # put all the country arguments together
    map_args = ["http://chart.apis.google.com/chart?cht=map:"
                "fixed=-60,-20,80,-35", "chs=600x400", "chma=0,60,0,0"]
    # set all the unselected countries to be gray
    cnts, clrs = ["AA"], ["808080", "808080"]
    # setup the colors for the legend
    for color in colors:
        cnts.append("AA")
        clrs.append(color)
    # add the countries
    for (color, country, val) in outputs:
        cnts.append(country.upper())
        if color >= len(colors):
            color = len(colors) - 1
        clrs.append(colors[color])
    map_args.append("chld=" + "|".join(cnts))
    map_args.append("chco=" + "|".join(clrs))

    # add the legend
    map_args.append("chdl=No+Data|" + "|".join(labels))
    map_args.append("chdls=000000,14")
    # add the background to make the graph more visible
    map_args.append("chf=bg,s,EFF3FF")

    # create the title
    title = title.replace(" ", "+")
    map_args.append("chtt=" + title)
    map_args.append("chts=000000,20,c")

    print "&".join(map_args)


def create_heat_map(data, x_labels, y_labels, filename, title=None,
                    size=(5, 3.5), inset_labs=None, font=None):
    fig, ax = plt.subplots()
    plt.subplot()
    cmap = plt.cm.hot
    cmap.set_bad(color='grey')

    img = ax.pcolormesh(data, cmap=cmap)
    cbar = plt.colorbar(img)

    ax = plt.subplot2grid((1, 10), (0, 0), colspan=9, rowspan=1)
    img = ax.pcolormesh(data, cmap=cmap, edgecolors='none')
    ax.set_xlim(0, data.shape[1])
    ax3 = plt.subplot2grid((1, 10), (0, 9), colspan=1, rowspan=3)
    cbar = plt.colorbar(img, cax=ax3, orientation="vertical")

    # setup a legend for the graph
    # cbar = plt.colorbar(cmap, ax=ax)
    # cbar = plt.colorbar(img)
    # cbar = fig.colorbar(img)
    # cbar.ax.get_yaxis().set_ticks([0.5, 2.5])
    # cbar.ax.set_yticklabels(["more censorship", "less censorship"])
    # cbar.ax.set_ylabel('Fraction of domains censored', rotation=270)
    ax3.get_yaxis().set_ticks([0.2, 0.8])
    if inset_labs is None:
        inset_labs = ["Less manipulation", "More manipulation"]
    ax3.set_yticklabels(inset_labs, rotation=90)
    # ax3.set_ylabel('Fraction of domains censored', rotation=270)

    # replace kids and teens with just kids if it is in the labels
    for index in range(len(x_labels)):
        if x_labels[index] == "kids_and_teens":
            x_labels[index] = "kids"

    # ax.set_frame_on(False)
    fig = plt.gcf()
    fig.set_size_inches(*size)

    # now setup the axes
    ax.invert_yaxis()
    # ax.xaxis.tick_top()
    # ax.set_xticklabels(x_labels, minor=False)
    # ax.set_yticklabels(y_labels, minor=False)

    # put the major ticks at the middle of each cell
    ax.set_xticks(np.arange(data.shape[1])+0.5, minor=False)
    ax.set_yticks(np.arange(data.shape[0])+0.5, minor=False)
    if font is not None:
        ax.set_xticklabels(x_labels, minor=False, fontsize=font, rotation=45,
                           ha='right')
    else:
        ax.set_xticklabels(x_labels, minor=False, rotation=45, ha='right')
    ax.set_yticklabels(y_labels, minor=False)
    # plt.xticks(rotation=45, ha='right')
    # ax.set_xticks(ax.get_xticks(rotation=45, ha='right')

    # ax2 = plt.subplot(1, 5, 5)
    # ax2 = ax
    # increments = np.outer(np.arange(0, 1, 0.01), np.ones(10))
    # ax2.axis("off")
    # ax2.imshow(increments, aspect='auto', cmap=cmap, origin='lower')
    # # setup a legend for the graph
    # ax2.get_yaxis().set_ticks([0.5, 2.5])
    # ax2.set_yticklabels(["more censorship", "less censorship"])
    # ax2.set_ylabel('Fraction of domains censored', rotation=270)

    # want a more natural, table-like display
    # ax.invert_yaxis()
    # x.xaxis.tick_top()

    # ax.xticks

    # setup the legend for the graph by creating a 10x100 pixel colormap
    # fig = plt.gcf()
    # fig.set_size_inches(8, 26)
    # ax1 = plt.subplot(1, 5, 1)
    # ax1.set_frame_on(False)
    # cmap = plt.cm.hot
    # cmap.set_bad(color='grey')
    # ax1.pcolormesh(data, cmap=cmap)
    # ax1.set_xticks(np.arange(data.shape[1])+0.5, minor=False)
    # ax1.set_yticks(np.arange(data.shape[0])+0.5, minor=False)
    # ax1.invert_yaxis()
    # ax1.set_xticklabels(x_labels, minor=False, rotation=45, ha='right')
    # ax1.set_yticklabels(y_labels, minor=False)
    # ax1.xticks(rotation=45, ha='right')

    if title is not None:
        plt.title(title)
    # plt.grid()
    plt.tight_layout()
    plt.savefig(filename, fmt='pdf')
    # plt.savefig(filename, fmt='png')


def create_pie_chart(data, title, filename, cutoff=0.01, verbose=False):
    """Create a pie chart from the given data

    Params:
    data- a collections.Counter where the keys should be the labels to
        the graph

    """
    total = float(sum(data.values()))
    frac_labels = []
    other_count = 0
    # compute the fraction for each label
    for cnt in data.keys():
        fraction = float(data[cnt]) / total
        if verbose:
            print "{}: {}".format(cnt, fraction)
        # if this slice is too small, just add it to other small
        # totals and display them together
        if fraction < cutoff:
            other_count += data[cnt]
            continue
        frac_labels.append((fraction, cnt))
    if other_count > 0:
        frac_labels.append((float(other_count) / total, "Other"))
    frac_labels.sort(key=lambda entry: entry[1])
    fractions, labels = zip(*frac_labels)
    plt.figure()
    plt.pie(fractions, labels=labels, autopct='%1.1f%%',
            colors=('b', 'g', 'r', 'c', 'm', 'y', 'k', 'w'))
    plt.title(title)
    plt.savefig(filename, fmt='pdf')


def create_scatter(datax, datay, x_label, y_label, filename, title=None,
                   log=False, set_same=False, colors=None,
                   num_colors=None, color_map=None, xlims=None, ylims=None):
    """Given a set of data, create a scatter plot of the data, optionally
    in log format

    """
    plt.figure()
    plt.grid()
    if colors is not None:
        plt.scatter(datax, datay, marker='x', c=colors, s=num_colors,
                    cmap=color_map)
    else:
        plt.scatter(datax, datay, marker='x')
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    if log:
        plt.xscale('log')

    if xlims is not None:
        plt.xlim(xlims)
    if ylims is not None:
        plt.ylim(ylims)

    if set_same:
        ylims = plt.ylim()
        plt.xlim(ylims)
    if title is not None:
        plt.title(title)
    plt.tight_layout()
    plt.savefig(filename, fmt='pdf')


def hist2d(x, y, ylabel=None, xlabel=None, title=None, filename=None,
           log=False, cmap=None, xlims=None, ylims=None, mincnt=1,
           numbins=100, c=None, reduce_c=None, color_lab=None):

    plt.figure()
    plt.grid()
    if c is None:
        if log:
            hexy = plt.hexbin(x, y, bins='log', mincnt=mincnt, gridsize=numbins,
                              cmap=cmap)
        else:
            hexy = plt.hexbin(x, y, mincnt=mincnt, gridsize=numbins, cmap=cmap)
    else:
        if log:
            hexy = plt.hexbin(x, y, bins='log', mincnt=mincnt, gridsize=numbins,
                              cmap=cmap, C=c, reduce_C_function=reduce_c)
        else:
            hexy = plt.hexbin(x, y, mincnt=mincnt, gridsize=numbins, cmap=cmap,
                              C=c, reduce_C_function=reduce_c)

    # have the colorbar give the original values, not the log
    lims = hexy.get_clim()
    ticks = np.linspace(lims[0], lims[1], 8)
    cb = plt.colorbar(ticks=ticks)
    if not color_lab:
        cb.set_label('counts')
    else:
        cb.set_label(color_lab)
    if log:
        new_labs = map(lambda x: "{}".format(int(math.pow(10, x) - 1)), ticks)
        cb.set_ticklabels(new_labs)

    # add in all of the optional stuff
    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)
    if title:
        plt.title(title)
    if xlims:
        plt.xlim(xlims)
    if ylims:
        plt.ylim(ylims)

    if filename:
        plt.savefig(filename)
    else:
        plt.show()


def scatter_density(x, y, xlabel=None, ylabel=None, title=None, xlims=None,
                    ylims=None, filename=None):
    plt.figure()
    plt.grid()
    hist, xedges, yedges = np.histogram2d(x, y)
    xidx = np.clip(np.digitize(x, xedges), 0, hist.shape[0] - 1)
    yidx = np.clip(np.digitize(y, yedges), 0, hist.shape[1] - 1)
    c = hist[xidx, yidx]
    print "starting to plot the scatter plot"
    plt.scatter(x, y, c=c)

    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)
    if title:
        plt.title(title)
    if xlims:
        plt.xlim(xlims)
    if ylims:
        plt.ylim(ylims)

    if filename:
        plt.savefig(filename)
    else:
        plt.show()


def test_graphing():
    """Create and save a graph to a temporary file to ensure that graphs
    can be saved

    """
    plt.close('all')
    fig, ax = plt.subplots()
    x = np.random.random(10)
    y = np.random.random(10)
    plt.plot(x, y)
    filep, filename = tempfile.mkstemp()
    os.close(filep)
    plt.savefig(filename, fmt='png')
    plt.savefig(filename, fmt='pdf')
    os.remove(filename)
    print "Successfully saved figures"


if __name__ == "__main__":
    test_graphing()