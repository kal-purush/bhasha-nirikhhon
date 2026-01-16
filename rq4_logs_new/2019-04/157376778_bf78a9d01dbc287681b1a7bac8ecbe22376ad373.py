import argparse
import os
from math import log
import matplotlib.pyplot as plt
from functools import reduce
import numpy as np
import csv

def parseFile(file):
    print("Processing ", file)
    peers = {}
    with open(file, 'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')
        next(plots)
        next(plots)
        next(plots)
        for row in plots:
            if not row[0] in peers: peers[row[0]] = []
            peers[row[0]].append(row)
    return peers

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plot an experiment built using tests/estimator.conf')
    parser.add_argument('path', metavar='path', type=str, help='Path to the experiment result file')
    args = parser.parse_args()
    pathtoexp = os.getcwd() + '/' + args.path
    peers = parseFile(pathtoexp)

    fig, ax = plt.subplots(figsize=(8, 6), nrows=1, ncols=1)
    xticks = []
    lasty = []
    for peer in peers:
        x = []
        y = []
        for row in peers[peer]:
            x.append(int(row[1]))
            y.append(int(row[4]))
        ax.scatter(x=x, y=y)
        xticks = x
        lasty = y
    plt.ylim(top=2000, bottom=0)
    #ax.set_yscale('log')
    ax.set_xlabel("Cycles")
    ax.set_ylabel("Network estimation size")
    #ax.set_title("Convergence time for the network size estimator for a network of 1000 peers")
    ax.axhline(y=1000, ls=(0, (1, 1)), color="red", label="Real size")
    ax.legend()
    fig.savefig(fname=args.path + '.png', quality=100, format='png', dpi=100)