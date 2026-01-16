from collections import defaultdict
import re

regex = re.compile("([a-z]+) (inc|dec) (-?[0-9]+) if ([a-z]+) ((?:>|<|!|=)=?) (-?[0-9]+)")

OPERATORS = {
    ">": "gt",
    "<": "lt",
    ">=": "ge",
    "<=": "le",
    "==": "eq",
    "!=": "ne",
}


def parse_line(line, registers):
    parsed_line = regex.match(line)

    parsed_target_register = parsed_line.group(1)
    parsed_instruction = parsed_line.group(2)
    parsed_value = parsed_line.group(3)
    parsed_condition_register = parsed_line.group(4)
    parsed_condition_operator = parsed_line.group(5)
    parsed_condition_value = parsed_line.group(6)

    condition_register_value = registers[parsed_condition_register]
    condition_comparison_function = "__%s__" % OPERATORS[parsed_condition_operator]
    condition_value = int(parsed_condition_value)

    if condition_register_value.__getattribute__(condition_comparison_function)(condition_value):
        if parsed_instruction == "inc":
            registers[parsed_target_register] += int(parsed_value)
        else:
            registers[parsed_target_register] -= int(parsed_value)

input_file = open("day_8_input")
registers = defaultdict(lambda: 0)
max_value = 0
for line in input_file:
    parse_line(line, registers)
    max_value_for_round = max(registers.values())
    if max_value_for_round > max_value:
        max_value = max_value_for_round

print(max(registers.values()))
print(max_value)