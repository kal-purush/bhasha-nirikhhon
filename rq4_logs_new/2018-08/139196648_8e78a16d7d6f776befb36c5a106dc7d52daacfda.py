#!/usr/bin/python3
import myPlotting
import numpy as np
import ROOT


def test_getXvalues():
    # hist
    hist = ROOT.TH1D("h", "", 10, 0, 10)
    np.testing.assert_array_almost_equal(myPlotting.getXvalues(hist),
                                         [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5])

    hist = ROOT.TH1D("h1", "", 6, -0.5, 5.5)
    np.testing.assert_array_almost_equal(myPlotting.getXvalues(hist), [0, 1, 2, 3, 4, 5])

    # TGraph
    graph = ROOT.TGraph()
    graph.SetPoint(0, 0, 1)
    np.testing.assert_array_almost_equal(myPlotting.getXvalues(graph), [0.0])
    graph.SetPoint(1, 0, 1)
    np.testing.assert_array_almost_equal(myPlotting.getXvalues(graph), [0.0, 0.0])
    graph.SetPoint(2, 10, 1)
    np.testing.assert_array_almost_equal(myPlotting.getXvalues(graph), [0.0, 0.0, 10])