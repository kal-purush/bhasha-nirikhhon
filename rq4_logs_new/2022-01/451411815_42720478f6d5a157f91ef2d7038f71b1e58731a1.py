# This script prints out sets of numbers that satisfy the equation p(x + y) = ab for integers

# Multiplier
p = 5

# Range to test
n = range(-1000, 1000)

print("X : Y \n")

for x in n:
    if x != p:
        y = ((-p)*x)/((-x)+p)
    if int(y) == y:
        print(x,":",y, "\n")