'''
    Given an array of integers, find the first missing positive integer in linear time and constant
        space. In other words, find the lowest positive integer that does not exist in the array.
        The array can contain duplicates and negative numbers as well.

    For example, the input [3, 4, -1, 1] should give 2. The input [1, 2, 0] should give 3.

    You can modify the input array in-place.
'''
import sys

SAMPLE_1 = [3, 4, -1, 1]
ANSWER_1 = 2

SAMPLE_2 = [1, 2, 0]
ANSWER_2 = 3

MAX_INT = 99
def main(data):
    '''
    Find the lowest missing integer.

    data is a list of integers
    '''
    data.sort(reverse=True)
    print("Sorted: {}".format(data))
    for index, datum in enumerate(data):
        print("datum: {}".format(datum))

        if index == 0:
            continue
        if datum < 0:
            continue

        if datum < data[index-1]-1:
            return datum+1

    return data[0]+1

if __name__ == "__main__":
    SMALLEST = main(SAMPLE_1)
    print("Smallest missing integer: {}".format(SMALLEST))
    sys.exit(0)