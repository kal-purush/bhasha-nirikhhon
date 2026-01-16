class Point():
    def __init__(self, x, velo):
        self.x = x
        self.velo = velo

    def move(self):
        self.x += self.velo

    def print(self):
        print(self.x, self.velo)

line = 10*[' ']

class Line():
    def __init__(self, size, points=None):
        self.line = size*[' ']

        if points is None:
            self.points = []
        self.points = points

    def step(self):
        for point in self.points:
            point.move()

    def draw(self):
        for point in self.points:
            self.line[point.x] = '*'

        print("".join(self.line))

        for point in self.points:
            self.line[point.x] = ' '

line = Line(10, [Point(0,1), Point(9,-1)])

# note how the code is much more readable
# common behavior is written in classes
for i in range(10):
    line.draw()
    line.step()

for point in line.points:
    point.print()

