def get_distance(input_position):
    return abs(input_position.imag) + abs(input_position.real)


with open('./day1.txt', 'r') as f:
    instructions = f.read().split(', ')
    position = 0 + 0j
    direction = 1j
    positions = set()
    repeated = None
    found_first_repeat = False

    for instruction in instructions:
        if instruction.startswith('R'):
            direction *= -1j
        else:
            direction *= 1j

        for _ in range(int(instruction[1:])):
            position += direction
            if position in positions and not found_first_repeat:
                repeated = position
                found_first_repeat = True

            positions.add(position)

    print(get_distance(position))
    print(get_distance(repeated))
