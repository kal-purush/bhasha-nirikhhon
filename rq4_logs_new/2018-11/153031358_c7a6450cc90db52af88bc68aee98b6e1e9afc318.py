import math

class Point:

    def __init__(self, x=0, y=0):
        self.move(x, y)

    def reset(self):
        self.x = 0
        self.y = 0
    
    def move(self, x, y):
        self.x = x
        self.y = y

    def calc_distance(self, other_point):
        return math.sqrt(
            (self.x - other_point.x)**2 +
            (self.y - other_point.y)**2)


p1 = Point()
p2 = Point()

assert (p1.calc_distance(p2) == p2.calc_distance(p1))
print(p1.calc_distance(p2))
print(p2.calc_distance(p1))

p1.move(3,4)
p2.move(8,9)

assert (p1.calc_distance(p2) == p2.calc_distance(p1))
print(p1.calc_distance(p2))
print(p2.calc_distance(p1))