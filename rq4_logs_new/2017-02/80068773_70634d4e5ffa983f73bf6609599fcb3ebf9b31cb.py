"""PLEASE - The Python Low-energy Electron Analysis SuitE.

Author: Maxwell Grady
Affiliation: University of New Hampshire Department of Physics Pohl group
Version 1.0.0
Date: February, 2017

Contained here are color definitions for use in color coding plots to
user selected areas in LEEM and LEED images.
"""

from PyQt5 import QtGui


class Palette(object):
    """Store color info for plotting in RGB and QColor modes."""

    def __init__(self):
        """Setup RGB list as 0->1.0 float and QColor in RGB int8 format."""
        self.color_palette = [(0.4, 0.76078, 0.64705),
                              (0.98823, 0.55294, 0.38431),
                              (0.55294, 0.62745, 0.79607),
                              (0.90588, 0.54117, 0.76470),
                              (0.65098, 0.84705, 0.32941),
                              (1.0, 0.85098, 0.18431),
                              (0.89804, 0.76862, 0.58039),
                              (0.70196, 0.70196, 0.70196),
                              (0.4, 0.76078, 0.64705),
                              (0.98823, 0.55294, 0.38431)]
        self.qcolors = [QtGui.QColor(int(255*tup[0]),
                                     int(255*tup[1]),
                                     int(255*tup[2])) for tup in self.color_palette]