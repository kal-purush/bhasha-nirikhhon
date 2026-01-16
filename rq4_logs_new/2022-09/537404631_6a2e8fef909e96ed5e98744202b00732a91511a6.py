class Base:
    _x = 0
    _y = 0

    def __init__(self, x, y):
        self.set(x, y)


    def get(self):
        return self._x, self._y


    def set(self, x, y):
        self._x = x
        self._y = y

a = Base(4,6)
b = Base(3,4)
print(*a.get())

a.set(4,6)
print(*a.get())