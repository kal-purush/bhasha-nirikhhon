import math

class Sphere(object):

    def __init__(self, radius):
        self.radius = radius

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, self.radius)

    def surface(self):
        return 4.0 * 3.1416 * self.radius ** 3
        
    def volume(self):
        return 4/3 * 3.1416 * self.radius ** 3
        
    def diameter(self):
        return self.radius