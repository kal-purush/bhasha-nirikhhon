from util import iohandler

# --- solution ---


def init(input_file):
    return [str(row.strip()) for row in input_file]


def get_accumulator_value(instruction_list):
    accumulator = 0
    exec_instruction_ids = list()
    instruction_pointer = 0

    while True:
        instruction = instruction_list[instruction_pointer].split(' ')
        if instruction_pointer in exec_instruction_ids:
            break
        else:
            exec_instruction_ids.append(instruction_pointer)

        if instruction[0] == 'nop':
            instruction_pointer += 1
        elif instruction[0] == 'acc':
            accumulator += int(instruction[1])
            instruction_pointer += 1
        elif instruction[0] == 'jmp':
            instruction_pointer += int(instruction[1])

    return accumulator


# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(get_accumulator_value(input_list)))