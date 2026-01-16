import numpy as np
#************************************************************************************************************************
#creating 2d grid
grid = []
dim = int(input("Enter the dimension of the game: "))
print(type(dim))
for row in range(dim):
    grid.append([])
    for column in range(dim):
        grid[row].append(int(np.random.binomial(1, 0.125, 1)))
#************************************************************************************************************************