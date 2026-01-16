#!/usr/bin/python3
"""bytecode explain"""

import math


class MagicClass:
    """representetion"""

    def __init__(self, radius=0):
        """inilization

        Arg:
            radius (float or int): the radius of megaclass
        """
        self.__radius = 0
        if type(radius) is not int and type(radius) is not float:
            raise TypeError("radius must be a number")
        self.__radius = radius

    def area(self):
        """Return the area"""
        return (self.__radius ** 2 * math.pi)

    def circumference(self):
        """Return The circumference"""
        return (2 * math.pi * self.__radius)