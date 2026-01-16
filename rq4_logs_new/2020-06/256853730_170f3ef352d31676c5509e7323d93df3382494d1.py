from typing import Generic, TypeVar

T = TypeVar('T')


class Node(Generic[T]):
    def __init__(self, value: T):
        self.next: Node[T] = None
        self.value: T = value


class List(Generic[T]):
    def __init__(self):
        self.head: Node[T] = None
        self.tail: Node[T] = None
        self.size: int = 0

    def push_front(self, e: T) -> None:
        raise NotImplementedError

    def push_back(self, e: T) -> None:
        raise NotImplementedError

    def pop_front(self) -> T:
        raise NotImplementedError

    def pop_back(self) -> T:
        raise NotImplementedError

    def get(self, index: int) -> T:
        raise NotImplementedError

    def get_at_index(self, e: T) -> Node:
        raise NotImplementedError

    def find(self, e: T) -> Node:
        raise NotImplementedError

    def is_empty(self) -> bool:
        return self.size == 0