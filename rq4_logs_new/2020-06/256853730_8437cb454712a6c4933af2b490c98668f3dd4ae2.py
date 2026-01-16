from typing import Generic, TypeVar

T = TypeVar('T')


class Vertex(Generic[T]):
    def __init__(self):
        self.is_visited = False
        self.id = id
        self.neighbours = set()

    def add_neighbour(self, v: 'Vertex'):
        self.neighbours.add(v)