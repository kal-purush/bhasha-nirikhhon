# -*- coding: utf-8 -*-

# Usage:
#
# Initialize
# q = Deque()
# q = Deque([10, 20])
# q = Deque([10, 20], 500000)
#
#
# append, pop
# q = Deque([10, 20])
# print(q) # Deque([10, 20])
# q.append(30)
# print(q) # Deque([10, 20, 30])
# q.appendleft(40)
# print(q) # Deque([40, 10, 20, 30])
# a = q.pop()
# print(q, a) # Deque([40, 10, 20]) 30
# b = q.popleft()
# print(q, b) # Deque([10, 20]) 40
#
#
# get, set
# q = Deque([10, 20, 30, 40, 50])
# a = q[1]
# print(q, a) # Deque([10, 20, 30, 40, 50]) 20
# q[1] = 60
# print(q) # Deque([10, 60, 30, 40, 50])
# b = q[-2]
# print(q, b) # Deque([10, 60, 30, 40, 50]) 40
# q[-2] = 70
# print(q) # Deque([10, 60, 30, 70, 50])
#
#
# type conversion etc
# to list:
# list(q)
#
# to set:
# set(q)
#
# list(reversed(q))
#
# len(q)


class Deque:
    """A dequeue that can perform random accesses with O(1).

    See:
    https://prd-xxx.hateblo.jp/entry/2020/02/07/114818
    """

    def __init__(self, src_arr=[], max_size=300000) -> None:
        self.size = max(max_size, len(src_arr)) + 1
        self.buf = list(src_arr) + [None] * (self.size - len(src_arr))
        self.head = 0
        self.tail = len(src_arr)

    def __index(self, i) -> int:
        length = len(self)

        if not -length <= i < length:
            raise IndexError("index out of range: " + str(i))
        if i < 0:
            i += length

        return (self.head + i) % self.size

    def __extend(self) -> None:
        ex = self.size - 1
        self.buf[self.tail + 1 : self.tail + 1] = [None] * ex
        self.size = len(self.buf)

        if self.head > 0:
            self.head += ex

    def is_full(self) -> bool:
        return len(self) >= self.size - 1

    def is_empty(self) -> bool:
        return len(self) == 0

    def append(self, x) -> None:
        if self.is_full():
            self.__extend()

        self.buf[self.tail] = x
        self.tail += 1
        self.tail %= self.size

    def appendleft(self, x) -> None:
        if self.is_full():
            self.__extend()

        self.buf[(self.head - 1) % self.size] = x
        self.head -= 1
        self.head %= self.size

    def pop(self):
        if self.is_empty():
            raise IndexError("pop() when buffer is empty")

        ret = self.buf[(self.tail - 1) % self.size]
        self.tail -= 1
        self.tail %= self.size

        return ret

    def popleft(self):
        if self.is_empty():
            raise IndexError("popleft() when buffer is empty")

        ret = self.buf[self.head]
        self.head += 1
        self.head %= self.size

        return ret

    def __len__(self) -> int:
        return (self.tail - self.head) % self.size

    def __getitem__(self, key):
        return self.buf[self.__index(key)]

    def __setitem__(self, key, value) -> None:
        self.buf[self.__index(key)] = value

    def __str__(self) -> str:
        return "Deque({0})".format(str(list(self)))