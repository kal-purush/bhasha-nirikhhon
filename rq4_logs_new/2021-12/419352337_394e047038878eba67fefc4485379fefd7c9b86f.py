import Grid


class Agregation:
    # agregacja H
    grid = None
    def __init__(self, grid:Grid.Grid):
        self.grid = grid
    def aggregateMatrix(self):
        H = [[0 for _ in range(self.grid.nN)] for _ in range(self.grid.nN)]
        for i in range(0, self.grid.nE):
            for j in range(4):
                for k in range(4):
                    H[self.grid.elements[i].id[j]-1][self.grid.elements[i].id[k]-1] += self.grid.elements[i].H[j][k]

        for item in H:
            print(item)
        print()

if __name__ == "__main__":
    pass