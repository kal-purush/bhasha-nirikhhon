#!/usr/bin/env python
# coding: utf-8
import matplotlib.pyplot as plt

from outils import *


def figure_sequence(seq, fig, bps=1):
    nb = {}
    for i in range(0, len(seq), bps):
        bits = seq[i:i + bps]
        val = binaire_vers_decimal(bits)
        if val in nb:
            nb[val] += 1
        else:
            nb[val] = 1
    p = {}
    labels = {}
    for (sym, count) in nb.items():
        p[sym] = 1.0 * count / len(seq) * 100
        labels[sym] = ("Symbole " + chaine_binaire(decimal_vers_binaire(sym, bps)))
    plt.figure(fig)
    plt.pie(p.values(), labels=labels.values(), autopct='%1.2f%%')
    s = ""
    for b in seq:
        s += str(b)
    print s
    plt.title(u"Séquence : {}".format(s))


def figure_chronogramme(x, y, titre, fig, xmin=None, xmax=None, ymin=None, ymax=None):
    figure(x, y, titre, "Temps (s)", "Tension (V)", fig, min(x) if xmin is None else xmin,
           max(x) if xmax is None else xmax, min(y) if ymin is None else ymin, max(y) if ymax is None else ymax)


def figure_spectre():
    pass


def figure_diagramme_oeil():
    pass


def figure_constellation():
    pass


def figure(x, y, titre, legende_x, legende_y, fig, xmin, xmax, ymin, ymax):
    plt.figure(fig)
    plt.plot(x, y)
    plt.axis([xmin, xmax, ymin, ymax])
    plt.xlabel(legende_x)
    plt.ylabel(legende_y)
    plt.title(titre)


def afficher():
    plt.show()