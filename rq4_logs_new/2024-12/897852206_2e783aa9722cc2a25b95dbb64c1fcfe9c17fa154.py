import csv
from typing import List


def get_reports() -> List[List[int]]:
    with open("day_two_input.csv") as input_file:
        reports: List[List[int]] = []
        input_reader = csv.reader(input_file)
        for row in input_reader:
            converted_values: List[int] = []
            for value in row[0].split(" "):
                converted_values.append(int(value))
            reports.append(converted_values)
    return reports


def is_report_safe(report: List[int]) -> bool:
    decreasing = True
    safe = True
    for i, level in enumerate(report):
        if i == 0:
            decreasing = report[i] - report[i + 1] > 0
        else:
            difference = report[i] - report[i - 1]
            if decreasing:
                safe = report[i - 1] > report[i] and -1 >= difference >= -3
            else:
                safe = report[i - 1] < report[i] and 1 <= difference <= 3

            if not safe:
                break
    return safe


def get_num_of_safe_reports(reports: List[List[int]]) -> int:
    num_of_safe_reports = 0
    for report in reports:
        if is_report_safe(report):
            num_of_safe_reports += 1
    return num_of_safe_reports


if __name__ == "__main__":
    reports: List[List[int]] = get_reports()
    print(get_num_of_safe_reports(reports))