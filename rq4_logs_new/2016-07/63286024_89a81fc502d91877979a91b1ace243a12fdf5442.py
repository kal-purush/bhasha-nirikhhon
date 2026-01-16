'''Contains shape classes and does fun things with them

Shapes:
Triangle
Square
Circle

Data Attributes:
shape_type
edge_length
name
allies
enemies

Methods:
area
perimeter
update_edge_length
add_ally
add_enemy
'''

import math

class Square(object):
    shape_type = 'square'

    def __init__(self, edge_length, name, allies, enemies):
        self.edge_length = edge_length
        self.name = name
        self.allies = allies
        self.enemies = enemies

    def area(self):
        return self.edge_length**2

    def perimeter(self):
        return self.edge_length * 4

    def update_edge_length(self, new_length):
        self.edge_length = new_length

    def add_ally(self, shape_object):
        self.allies.append(shape_object)

    def add_enemy(self, shape_object):
        self.enemies.append(shape_object)

class Triangle(object):
    shape_type = 'triangle'

    def __init__(self, edge_length, name, allies, enemies):
        self.edge_length = edge_length
        self.name = name
        self.allies = allies
        self.enemies = enemies

    def area(self):
        return ((3^0.5)/4) * self.edge_length * self.edge_length

    def perimeter(self):
        return self.edge_length * 4

    def update_edge_length(self, new_length):
        self.edge_length = new_length

    def add_ally(self, shape_object):
        self.allies.append(shape_object)

    def add_enemy(self, shape_object):
        self.enemies.append(shape_object)

class Circle(object):
    shape_type = 'circle'

    def __init__(self, radius, name, allies, enemies):
        self.radius = radius
        self.name = name
        self.allies = allies
        self.enemies = enemies

    def area(self):
        return math.pi * self.radius * self.radius

    def update_radius(self, new_radius):
        self.radius = new_radius

    def add_ally(self, shape_object):
        self.allies.append(shape_object)

    def add_enemy(self, shape_object):
        self.enemies.append(shape_object)

if __name__ == '__main__':
    square_marty = Square(5, "marty", [], [])
    print(square_marty.area())
    print(square_marty.update_edge_length(10))
    print(square_marty.allies)

    triangle_joe = Triangle(3, "joe", ["marty"], ["susan"])
    print(triangle_joe.enemies)

    circle_susan = Circle(2, "susan", ["marty", "joe"], [])
    print("Susan's Area: {}    Susan's Allies: {}".format(circle_susan.area(), circle_susan.allies))