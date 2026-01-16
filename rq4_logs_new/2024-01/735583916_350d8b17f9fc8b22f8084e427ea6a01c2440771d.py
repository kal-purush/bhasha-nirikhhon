import sympy as sp

with open('input/day24.txt', 'r') as f:
    puzzle_input = f.read()


def part2(puzzle_input):
    first_three_hailstones = []
    for line in puzzle_input.split('\n')[:3]:
        nums = line.replace('@', ',').split(',')
        first_three_hailstones.append(tuple(map(int, nums)))

    unknowns = sp.symbols('x y z vx vy vz t0 t1 t2')
    x, y, z, dx, dy, dz, *time = unknowns

    equations = []  # build system of 9 equations with 9 unknowns
    for t, h in zip(time, first_three_hailstones):
        equations.append(sp.Eq(x + t*dx, h[0] + t*h[3]))
        equations.append(sp.Eq(y + t*dy, h[1] + t*h[4]))
        equations.append(sp.Eq(z + t*dz, h[2] + t*h[5]))

    solution = sp.solve(equations, unknowns).pop()
    return sum(solution[:3])


print(part2(puzzle_input))