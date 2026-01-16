from util import iohandler
import time

# --- solution ---


def init(input_file):
    return [str(row.strip()) for row in input_file]


# it's kind of magic (aka bruteforce)
def get_fixed_accumulator_value(instruction_list):
    accumulator = None
    fixable_indexes = set()

    for index in range(len(instruction_list)):
        if instruction_list[index].split(' ')[0] in ('jmp', 'nop'):
            fixable_indexes.add(index)

    while accumulator is None:
        instruction_list_cpy = instruction_list.copy()
        fix_index = fixable_indexes.pop()
        inst = instruction_list_cpy[fix_index][:3]

        if inst == 'jmp':
            instruction_list_cpy[fix_index] = instruction_list_cpy[fix_index].replace('jmp', 'nop')
        else:
            instruction_list_cpy[fix_index] = instruction_list_cpy[fix_index].replace('nop', 'jmp')

        accumulator = loop_it(instruction_list_cpy)

    return accumulator


def loop_it(instruction_list):
    accumulator = 0
    exec_instruction_ids = list()
    instruction_pointer = 0
    timeout = time.time() + 2

    while True:
        if instruction_pointer >= len(instruction_list):
            return accumulator

        if time.time() > timeout:
            return None

        instruction = instruction_list[instruction_pointer].split(' ')
        if instruction_pointer in exec_instruction_ids:
            return None
        else:
            exec_instruction_ids.append(instruction_pointer)

        if instruction[0] == 'nop':
            instruction_pointer += 1
        elif instruction[0] == 'acc':
            accumulator += int(instruction[1])
            instruction_pointer += 1
        elif instruction[0] == 'jmp':
            instruction_pointer += int(instruction[1])


# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(get_fixed_accumulator_value(input_list)))