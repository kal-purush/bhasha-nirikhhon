# import math
#
#
# class Vector2d:
#     def __init__(self, x, y):
#         self.x = float(x)
#         self.y = float(y)
#
#     def __repr__(self):
#         return 'Vector2d(%r, %r)' % (self.x, self.y)
#
#     def __iter__(self):
#         return (i for i in (self.x, self.y))
#
#     def __str__(self):
#         return '(%r, %r)' % (self.x, self.y)
#
#     def __eq__(self, other):
#         return tuple(self) == tuple(other)
#
#     def __abs__(self):
#         return math.sqrt(self.x**2+self.y**2)
#
#     def __bool__(self):
#         if self.x == 0 and self.y == 0:
#             return False
#         else:
#             return True


class Node(object):
    def __init__(self, elem=-1, left_child=None, right_child=None):
        self.elem = elem
        self.left = left_child
        self.right = right_child


class Tree(object):
    def __init__(self):
        self.root = Node()

    def add(self, elem):
        node = Node(elem)
        print(self.root.elem)