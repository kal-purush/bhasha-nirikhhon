from collections import defaultdict
from aoc2024.utils.benchmark import timer
from aoc2024.utils.reader import read_input


IS_TEST = False
lines = read_input(IS_TEST)


def mix_prune(number: int, secret: int) -> int:
    return (number & 0xFFFFFF) ^ secret


def step(secret: int) -> int:
    secret = mix_prune(secret << 6, secret)
    secret = mix_prune(secret >> 5, secret)
    secret = mix_prune(secret << 11, secret)
    return secret


def steps(secret: int, count: int) -> int:
    for _ in range(count):
        secret = step(secret)
    return secret


@timer
def part1() -> None:
    secrets = [steps(int(line), 2000) for line in lines]
    result = sum(secrets)
    print(f"Part1: {result}")


def last_digits(secret: int, count: int) -> list[int]:
    digits = [secret % 10]
    for _ in range(count):
        secret = step(secret)
        digits.append(secret % 10)
    return digits


def process_digits(digits: list[int]) -> dict[tuple[int, int, int, int], int]:
    diff_tuples = [
        (
            (
                digits[i - 3] - digits[i - 4],
                digits[i - 2] - digits[i - 3],
                digits[i - 1] - digits[i - 2],
                digits[i - 0] - digits[i - 1],
            ),
            digits[i],
        )
        for i in range(4, len(digits))
        #
    ]
    result = {}
    for diff, res in diff_tuples:
        if diff not in result:
            result[diff] = res
    return result


@timer
def part2() -> None:
    all_diff_dict: dict[tuple[int, int, int, int], int] = defaultdict(int)
    for line in lines:
        secret = int(line)
        digits = last_digits(secret, 2000)
        diff_dict = process_digits(digits)
        for diff, res in diff_dict.items():
            all_diff_dict[diff] += res
    max_value = max(all_diff_dict.values())
    print(f"Part2: {max_value}")


if __name__ == "__main__":
    part1()
    part2()