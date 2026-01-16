#!/usr/bin/python3
"""
A script to calculate the fewest number of operations needed
to result in exactly n H characters in the file.
"""


def minOperations(target):
    """
    Calculates the minimum number of operations needed
    to achieve the target number of characters in the file.

    :param target: The desired number of characters.
    :type target: int
    :return: The minimum number of operations required.
    :rtype: int
    """
    if target <= 1:
        return 0

    current = 1
    clipboard = current
    mN = 0

    while current < target:
        if target % current == 0:
            clipboard = current
            current = 2 * clipboard
            mN += 2
        else:
            current += clipboard
            mN += 1

    return mN