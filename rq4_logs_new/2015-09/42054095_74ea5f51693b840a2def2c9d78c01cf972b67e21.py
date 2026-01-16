#!/usr/bin/env python3
from collections import defaultdict

__author__ = 'dvorkjoker,jodaiber'
"""
Script to train weights for features for decomposition lattice.
"""

import ast

def edge_to_split(edge):
    return edge[0], edge[1]

class Lattice(object):
    def __init__(self, arg):

        self.arcs_to = defaultdict(list)
        self.arc_offsets = {}

        # parse from string, if needed
        if isinstance(arg, str):
            arg = ast.literal_eval(arg)

        self.lattice = arg
        self.lattice.setdefault(list)

        self.edges = set()
        for (key, edges) in self.lattice.items():
            for edge in edges:
                (edge_from, edge_to, prefix, rank, similarity) = edge
                self.arcs_to[edge_to].append(edge)
                self.edges += edge

    def get_splits(self):  # [(0,3), ...]
        return sorted(set(map(edge_to_split, self.edges)))

    def splits_from(self, i):
        return map(edge_to_split, self.lattice[i])

    def splits_to(self, i):
        return map(edge_to_split, self.arcs_to[i])