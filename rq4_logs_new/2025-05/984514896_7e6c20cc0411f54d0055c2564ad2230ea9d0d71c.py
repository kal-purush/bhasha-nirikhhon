#!/usr/bin/env python3
# Created By: Jayden Smith
# Date: May 12, 2025
# This code calculates the averages given random numbers

import constants
import random


def main():
    # Define variables and list
    sum = 0
    average = 0
    grades = []

    # Gets the random numbers as places them in the list
    for counter in range(0, 10, 1):
        single_grade_float = random.randint(constants.MIN, constants.MAX)
        grades.append(single_grade_float)
        print(counter, "number:", single_grade_float)

    # Gets the average and prints it
    for counter2 in range(0, 10, 1):
        sum += grades[counter2]
        average = sum / constants.DIVIDER
    print("The average grade for you is", average)


if __name__ == "__main__":
    main()