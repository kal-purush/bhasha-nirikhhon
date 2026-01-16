import numpy as np
import math


class Color:
    __slots__ = ['red', 'green', 'blue']

    def __init__(self, red, green, blue):
        if not all(isinstance(arg, int) for arg in [red, green, blue]):
            raise TypeError('expected an int.')

        self.red = red
        self.green = green
        self.blue = blue

    def __repr__(self):
        return 'Color({0.red}, {0.green}, {0.blue})'.format(self)
    
    def get(self):
        return (self.red, self.green, self.blue)

    def linear_interpolate(self, color2, percent):
        red = math.ceil(self.red + percent * (color2.red - self.red))
        green = math.ceil(self.green + percent * (color2.green - self.green))
        blue = math.ceil(self.blue + percent * (color2.blue - self.blue))

        return Color(red, green, blue)


class Palette:
    def __init__(self, src):
        self.colors = []

        try:
            with open(src, 'r') as f:
                colors = (tuple(map(int, row.split(' ')) for row in f))
                
                for red, green, blue in colors:
                    self.colors.append(Color(red, green, blue))
        except FileNotFoundError:
            raise FileNotFoundError('{!r} was not found'.format(src))
    
    def __getitem__(self, index):
        return self.colors[index]


if __name__ == '__main__':
    palette = Palette('colors.txt')
    print(palette[0])
