with open('./day3.txt', 'r') as f:
    count = 0
    all_sides = []
    for line in f:
        sides = [int(each) for each in line.strip().split()]
        all_sides.append(sides)

    for i in range(0, len(all_sides), 3):
        for j in range(3):
            sides_from_column = [all_sides[i][j], all_sides[i + 1][j], all_sides[i + 2][j]]
            sorted_sides = sorted(sides_from_column)

            if sorted_sides[0] + sorted_sides[1] > sorted_sides[2]:
                count += 1

    print(count)