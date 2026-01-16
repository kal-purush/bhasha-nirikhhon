from tkinter import Tk, Canvas
from multiplication_table.process_math.Graph import Graph 
from multiplication_table.process_vis.edges_vis import one_edge, all_edges
from multiplication_table.process_vis.base_vis import angle_tab


def test_all_edges():
    cnv = Canvas(Tk(), width=600, height=600)
    assert type(all_edges(canvas=cnv, graph=Graph(2,10), radius=50,
                          center=300, color_graph=["red"],
                          edges_width=2)) == Canvas


def test_one_edge():
    cnv = Canvas(Tk(), width=600, height=600)
    angle = angle_tab(200, Graph(2, 10))
    assert type(one_edge(canvas=cnv, graph=Graph(2,10), i=1, angle=angle,
                          center=300, color_graph=["red"],
                          edges_width=2)) == Canvas