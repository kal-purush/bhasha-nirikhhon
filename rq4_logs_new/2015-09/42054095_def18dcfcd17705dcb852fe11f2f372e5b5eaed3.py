#!/usr/bin/env python3
__author__ = 'dvorkjoker'
"""
Script to train weights for features for decomposition lattice.
"""

import fst
import ast

def dot(seq1, seq2):
    assert(len(seq1) == len(seq2))
    return sum(x[0]*x[1] for x in zip(seq1, seq2))

def acceptor_from_edges(edges, weights):
    result = fst.Acceptor()
    for key, value in edges.items():
        for entry in value:
            to, label, features = entry
            result.add_arc(key, to, label, dot(features, weights))
    result[1].final = True
    return result

def lattice_to_split(viterbi_lattice):
    result = []
    for vertix in viterbi_lattice:
        if not vertix.final:
            for i, arc in enumerate(vertix.arcs):
                assert(i == 0)
                result += [viterbi_lattice.isyms.find(arc.ilabel)]
    return result
        

class Lattice(object):
    def __init__(self, arg):
        # parse from string, if needed
        if isinstance(arg, str):
            arg = ast.literal_eval(arg)

        self._lattice = arg

    def get_full(self, weights):
        return acceptor_from_edges(self._lattice, weights)

    def get_viterbi(self, weights):
        lattice = acceptor_from_edges(self._lattice, weights)
        result = lattice.shortest_path()
        result.top_sort()
        return result
