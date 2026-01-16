import re
from typing import List

# mul() commands should have 2 valid numbers as parameters, e.g. mul(20, 250), mul(1, 12345)
MUL_COMMAND_REGEX = "mul\(\d+,\d+\)"


def get_data() -> str:
    with open("day_three_input.txt") as input_file:
        return input_file.read()


def get_sum_of_products_from_mul_commands(mul_commands: List[str]) -> int:
    result = 0
    for mul_command in mul_commands:
        values = re.findall("\d+", mul_command)
        if len(values) != 2:
            raise ValueError("Issue determining which values to multiply")
        product = int(values[0]) * int(values[1])
        result += product
    return result


if __name__ == "__main__":
    data = get_data()
    mul_commands = re.findall(MUL_COMMAND_REGEX, data)
    result = get_sum_of_products_from_mul_commands(mul_commands)
    print(result)