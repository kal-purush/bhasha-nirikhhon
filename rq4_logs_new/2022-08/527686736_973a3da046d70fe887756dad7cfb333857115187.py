from typing import List, Union

number = Union[int, float]


def average(scores: List[number]) -> number:
    total_score: number = 0
    for score in scores:
        total_score += score

    return total_score / len(scores)