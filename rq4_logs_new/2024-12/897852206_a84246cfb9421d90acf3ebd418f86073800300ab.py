import csv
from typing import List

list_one: List[int] = []
list_two: List[int] = []


def read_input_lists() -> None:
    with open("day_one_input.csv") as input_file:
        input_reader = csv.reader(input_file, delimiter=' ', skipinitialspace=True)
        for row in input_reader:
            list_one.append(int(row[0]))
            list_two.append(int(row[1]))


def calculate_similarity_score() -> int:
    similarity_score: int = 0
    for value in set(list_one):
        occurrences_of_value: int = list_two.count(value)
        similarity_score += value * occurrences_of_value
    return similarity_score


if __name__ == "__main__":
    read_input_lists()
    similarity_score: int = calculate_similarity_score()
    print(similarity_score)