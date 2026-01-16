keys_part1 = [['1', '2', '3'],
              ['4', '5', '6'],
              ['7', '8', '9']]
keys_part2 = [['x', 'x', '1', 'x', 'x'],
             ['x', '2', '3', '4', 'x'],
             ['5', '6', '7', '8', '9'],
             ['x', 'A', 'B', 'C', 'x'],
             ['x', 'x', 'D', 'x', 'x']]
x = 2
y = 0
code = []


def is_valid(x, y, keys):
    return 0 <= x < 5 and 0 <= y < 5 and keys[x][y] != 'x'


with open('./day2.txt', 'r') as f:
    for line in f:
        instructions = list(line.strip())

        for instruction in instructions:
            if instruction == 'U':
                if is_valid(x - 1, y, keys_part2):
                    x -= 1
            elif instruction == 'L':
                if is_valid(x, y - 1, keys_part2):
                    y -= 1
            elif instruction == 'D':
                if is_valid(x + 1, y, keys_part2):
                    x += 1
            else:
                if is_valid(x, y + 1, keys_part2):
                    y += 1

        code.append(keys_part2[x][y])

print(''.join(code))
