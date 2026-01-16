from collections import Counter
import json
import os
from pathlib import Path
import sys

import MDAnalysis as mda
import numpy as np
import networkx as nx
import pytest


parent_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(parent_dir))

import grodecoder as gd


@pytest.mark.parametrize(
    "path_structure_file, list_atom_id_remain_reference",
    [ ("data/fake_hydrogen.gro", np.array([1,5,9,13,14,16,18,20,23,26,30,35,38,41,42])), 
      ("data/fake_hydrogen.pdb", np.array([1,2,3,4,5,6,7,12,16,19,21]) ),
    ],
)
def test_remove_hydrogene(path_structure_file, list_atom_id_remain_reference):
    molecular_system_without_hydrogren = gd.remove_hydrogene(path_structure_file)
    assert np.array_equal(molecular_system_without_hydrogren.atoms.ids, list_atom_id_remain_reference) == True


@pytest.mark.parametrize(
    "path_structure_file, formula_without_h_reference",
    [ ("data/get_formula_waterTIP3.gro", "O"),
      ("data/get_formula_CHL1.gro", "C27O"),
      ("data/get_formula_DOPC.gro", "C44NO8P"),
      ("data/get_formula_POPP2.gro", "C37O11P2"),
    ],
)
def test_get_formula(path_structure_file, formula_without_h_reference):
    molecular_universe = mda.Universe(path_structure_file)
    atom_names = Counter(molecular_universe.atoms.names)
    atom_names = dict(sorted(atom_names.most_common()))

    formula = gd.get_formula_based_atom_name(atom_names)
    assert formula == formula_without_h_reference


@pytest.mark.parametrize(
    "sort_list_integers, reference_interval",
    [ ([1, 2, 3, 6, 7, 8, 10], ['1-3', '6-8', '10']), 
      ([2,5,6,7,11,12,13,14], ['2', '5-7', '11-14']),
    ],
)
def test_get_intervals(sort_list_integers, reference_interval):
    interval = gd.get_intervals(sort_list_integers)
    assert interval == reference_interval


G1 = nx.Graph()
G1.add_edges_from([(1, 2), (3, 2), (2, 4), (4, 5)])
nx.set_node_attributes(G1, {1: 1, 2: 1, 3: 2, 4: 2, 5: 2}, name="residue_id")
nx.set_node_attributes(G1, {1: "ALA", 2: "ALA", 3: "CYS", 4: "CYS", 5: "CYS"}, name="residue_name")
nx.set_node_attributes(G1, {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}, name="atom_id")

G2 = nx.Graph()
G2.add_edges_from([(1, 2), (3, 2), (2, 4), (4, 5)])
nx.set_node_attributes(G2, {1: 1, 2: 1, 3: 2, 4: 2, 5: 5}, name="residue_id")
nx.set_node_attributes(G2, {1: "ALA", 2: "ALA", 3: "CYS", 4: "CYS", 5: "GLY"}, name="residue_name")
nx.set_node_attributes(G2, {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}, name="atom_id")

@pytest.mark.parametrize(
    "graph, reference_dict_interval",
    [ (G1, {"res_id": [1,2], "res_id_interval": ['1-2'], "atom_id": [1,2,3,4,5], "atom_id_interval": ['1-5']}), 
      (G2, {"res_id": [1,2,5], "res_id_interval": ['1-2', '5'], "atom_id": [1,2,3,4,5], "atom_id_interval": ['1-5']}),
    ],
)
def test_extract_interval(graph, reference_dict_interval):
    dict_interval = gd.extract_interval(graph)
    key_to_test = ["res_id", "res_id_interval", "atom_id", "atom_id_interval"]
    for key in key_to_test:
        print(type(dict_interval[key]), type(reference_dict_interval[key]))
        assert list(dict_interval[key]) == reference_dict_interval[key]


G3 = nx.Graph()
G3.add_edges_from([(1, 2), (3, 2)])
nx.set_node_attributes(G3, {1: "ADG", 2: "ADG", 3: "ADG"}, name="residue_name")

G4 = nx.Graph()
G4.add_edges_from([(1, 2), (3, 2)])
nx.set_node_attributes(G4, {1: "DFPE", 2: "DFPE", 3: "DFPE"}, name="residue_name")

G5 = nx.Graph()
G5.add_edges_from([(1, 2), (3, 2)])
nx.set_node_attributes(G5, {1: "ALA", 2: "ALA", 3: "ALA"}, name="residue_name")

@pytest.mark.parametrize(
    "resolution, graph, dict_count, bool_lipid_reference",
    [ ("AA", G3, {"formula_no_h": "C16O6"}, True), 
      ("CG", G4, {}, True),
      ("AA", G5, {"formula_no_h": ""}, False),
    ],
)
def test_is_lipid(resolution, graph, dict_count, bool_lipid_reference):
    bool_lipid = gd.is_lipid(resolution, graph, dict_count)
    assert bool_lipid == bool_lipid_reference


G6 = nx.Graph()
G6.add_edges_from([(1, 2), (3, 2), (2, 4), (4, 5)])
nx.set_node_attributes(G6, {1: "ALA", 2: "ARG", 3: "CYS", 4: "LYS", 5: "ILE"}, name="residue_name")

G7 = nx.Graph()
G7.add_edges_from([(1, 2), (3, 2), (2, 4), (4, 5)])
nx.set_node_attributes(G7, {1: "HIT", 2: "BAD", 3: "NIL", 4: "CIL", 5: "ALS"}, name="residue_name")

G8 = nx.Graph()
G8.add_edges_from([(1, 2), (3, 2)])
nx.set_node_attributes(G8, {1: "PHE", 2: "CYS", 3: "LYS"}, name="residue_name")

@pytest.mark.parametrize(
    "graph, bool_protein_reference",
    [ (G6, True), 
      (G7, False),
      (G8, False),
    ],
)
def test_is_protein(graph, bool_protein_reference):
    bool_protein = gd.is_protein(graph)
    assert bool_protein == bool_protein_reference


@pytest.mark.parametrize(
    "graph, reference_dict",
    [ (G1, {"sequence": "AC", 'nb_res': 2}), 
      (G2, {"sequence": "ACG", 'nb_res': 3}),
    ],
)
def test_extract_protein_sequence(graph, reference_dict):
    dict_test = gd.extract_protein_sequence(graph)
    assert dict_test == reference_dict


G9 = nx.Graph()
G9.add_edges_from([(1, 2)])
nx.set_node_attributes(G9, {1: "MET", 2: "MET"}, name="residue_name")

@pytest.mark.parametrize(
    "graph, reference_bool_met",
    [ (G9, True), 
      (G5, False),
    ],
)
def test_is_met(graph, reference_bool_met):
    bool_met = gd.is_met(graph)
    assert bool_met == reference_bool_met


@pytest.mark.parametrize(
    "info_molecule, path_structure_file, counts, solvant_or_ion, list_atom_id_remain",
    [ ({"name": "chloride ion", "res_name": "CLA", "atom_names": ["CLA"]}, "data/find_ion_CLA.gro", {}, "ion", np.array([1,2,3,4,5,7,8,9])), 
      ({"name": "water TIP3P solvant", "res_name": "SOL", "atom_names": ["OH2", "H1", "H2"]}, "data/find_ion_CLA.gro", {}, "solvant", np.array([1,2,3,4,5,6])), 
      ({"name": "acetonitrile", "res_name": "ACN", "atom_names": ["C1", "C2", "N"]}, "data/find_ion_CLA.gro", {}, "solvant", np.array([1,2,3,4,5,6,7,8,9])), 
    ],
)
def test_find_ion_solvant(info_molecule, path_structure_file, counts, solvant_or_ion, list_atom_id_remain):
    universe = mda.Universe(path_structure_file)
    universe_test, count_test = gd.find_ion_solvant(info_molecule, universe, counts, solvant_or_ion)
    np.array_equal(universe_test.atoms.ids, list_atom_id_remain) == True


@pytest.mark.parametrize(
    "path_structure_file, reference_resolution",
    [ ("data/test_guess_resolution_AA.gro", "AA"), 
      ("data/test_guess_resolution_CG.gro", "CG"),
    ],
)
def test_guess_resolution(path_structure_file, reference_resolution):
    universe = mda.Universe(path_structure_file)
    resolution_test = gd.guess_resolution(universe)
    assert resolution_test == reference_resolution