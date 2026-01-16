import pytest, pdb
import numpy as np
import time, os

from ..node import Node
from ..workflow import Workflow
from ..auxiliary import Function_Interface

from ... import config, logging
config.enable_debug_mode()
logging.update_logging(config)

def funA(a):
    print("A Before Waiting")
    time.sleep(5)
    print("A After Waiting")
    return a**2

def funB(b):
    return b+2

def funC(c):
    return 10 * c

def funD(d1, d2):
    print("IM IN D", d1, d2)
    return d1 + d2

def funE(e1, e2):
    return e1 * e2

def funF():
    return 0


def test_workflow_mapper_1():
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")

    wf = Workflow(nodes=[nA], name="workflow_1", workingdir="test_mapper_1")
    wf.run()

    expected = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25),
                ({"a": 6}, 36), ({"a": 7}, 49), ({"a": 8}, 64)]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]


def test_workflow_mapper_1a():
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5]), "d2": np.array([10, 20, 30])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]),
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_1a", workingdir="test_mapper_1a")
    wf.run()

    expected = [({"d1":3, "d2": 10}, 13), ({"d1":4, "d2":20}, 24), ({"d1":5, "d2":30}, 35)]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]


def test_workflow_mapper_2():
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nB = Node(inputs={"b": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB", plugin="mp")

    wf = Workflow(nodes=[nA, nB], name="workflow_2", workingdir="test_mapper_2")
    wf.run()

    expected = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25),
                ({"a": 6}, 36), ({"a": 7}, 49), ({"a": 8}, 64)]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    assert nB.result["out"][0][0] == {"b": 15}
    assert nB.result["out"][0][1] == 17


def test_workflow_mapper_3():
    """graph: A -> B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC", plugin="mp")

    wf = Workflow(nodes=[nA, nC], name="workflow_3", workingdir="test_mapper_3")
    wf.connect(nA, "out", nC, "c")
    wf.run()

    expected_A = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25),
                 ({"a": 6}, 36), ({"a": 7}, 49), ({"a": 8}, 64)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    expected_C = [({"a": 3}, 90), ({"a": 4}, 160), ({"a": 5}, 250),
                  ({"a": 6}, 360), ({"a": 7}, 490), ({"a": 8}, 640)]
    for i, res in enumerate(expected_C):
        assert nC.result["out"][i][0] == res[0]
        assert nC.result["out"][i][1] == res[1]


def test_workflow_mapper_4():
    """graph: A -> D"""
    nA = Node(inputs={"a": np.array([3, 4, 5])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nD = Node(inputs={"d1": np.array([10, 20, 30])}, mapper=("a", "d1"),
              interface=Function_Interface(funD, ["out"]),
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nA, nD], name="workflow_4", workingdir="test_mapper_4")
    wf.connect(nA, "out", nD, "d2")
    wf.run()

    expected_A = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    expected_D = [({"a":3, "d1":10}, 19), ({"a":4, "d1":20}, 36), ({"a":5, "d1":30}, 55)]
    for i, res in enumerate(expected_D):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]


def test_workflow_mapper_5():
    """graph: A -> C, A -> D,  B -> D, F"""
    nA = Node(inputs={"a": np.array([3, 5])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nB = Node(inputs={"b": np.array([10, 20])}, mapper="b",
              interface=Function_Interface(funB, ["out"]),
              name="nB", plugin="mp")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC", plugin="mp")
    nD = Node(interface=Function_Interface(funD, ["out"]),
              name="nD", plugin="mp", mapper=("a", "b"))
    nF = Node(interface=Function_Interface(funF, ["out"]),
              name="nF", plugin="mp")

    wf = Workflow(nodes=[nA, nB, nC, nD, nF], name="workflow_5", workingdir="test_mapper_5")
    wf.connect(nA, "out", nC, "c")
    wf.connect(nA, "out", nD, "d1")
    wf.connect(nB, "out", nD, "d2")
    wf.run()

    expected_A = [({"a":3}, 9), ({"a":5}, 25)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    expected_B = [({"b":10}, 12), ({"b":20}, 22)]
    for i, res in enumerate(expected_B):
        assert nB.result["out"][i][0] == res[0]
        assert nB.result["out"][i][1] == res[1]

    expected_C = [({"a": 3}, 90), ({"a": 5}, 250)]
    for i, res in enumerate(expected_C):
        assert nC.result["out"][i][0] == res[0]
        assert nC.result["out"][i][1] == res[1]


    expected_D = [({"a":3, "b":10}, 21), ({"a":5, "b":20}, 47)]
    for i, res in enumerate(expected_D):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    assert nF.result["out"][0][0] == {}
    assert nF.result["out"][0][1] == 0


#doesnt work
def test_workflow_mapper_6():
    """graph: A -> C, A -> D,  B -> D, C -> E, D -> E, F"""
    nA = Node(inputs={"a": np.array([3, 5])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nB = Node(inputs={"b": np.array([10, 20])}, mapper="b",
              interface=Function_Interface(funB, ["out"]),
              name="nB", plugin="mp")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC", plugin="mp")
    nD = Node(interface=Function_Interface(funD, ["out"]),
              name="nD", plugin="mp", mapper=("a", "b"))
    nE = Node(interface=Function_Interface(funE, ["out"]),
              name="nE", plugin="mp", mapper=("a", "b"))
    nF = Node(interface=Function_Interface(funF, ["out"]),
              name="nF", plugin="mp")

    wf = Workflow(nodes=[nA, nB, nC, nD, nE, nF], name="workflow_6", workingdir="test_mapper_6")
    wf.connect(nA, "out", nC, "c")
    wf.connect(nA, "out", nD, "d1")
    wf.connect(nB, "out", nD, "d2")
    wf.connect(nC, "out", nE, "e1")
    wf.connect(nD, "out", nE, "e2")
    wf.run()

    expected_A = [({"a":3}, 9), ({"a":5}, 25)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    expected_B = [({"b":10}, 12), ({"b":20}, 22)]
    for i, res in enumerate(expected_B):
        assert nB.result["out"][i][0] == res[0]
        assert nB.result["out"][i][1] == res[1]

    expected_C = [({"a": 3}, 90), ({"a": 5}, 250)]
    for i, res in enumerate(expected_C):
        assert nC.result["out"][i][0] == res[0]
        assert nC.result["out"][i][1] == res[1]

    expected_D = [({"a":3, "b":10}, 21), ({"a":5, "b":20}, 47)]
    for i, res in enumerate(expected_D):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_E = [({"a":3, "b":10}, 1890), ({"a":5, "b":20}, 11750)]
    for i, res in enumerate(expected_E):
        assert nE.result["out"][i][0] == res[0]
        assert nE.result["out"][i][1] == res[1]

    assert nF.result["out"][0][0] == {}
    assert nF.result["out"][0][1] == 0