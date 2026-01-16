#********************************************************************
# Filename:  BinarySearch.py
# Author:    Javier Montenegro (https://javiermontenegro.github.io/)
# Copyright:
# Details:   This code is the implementation of the binary search algorithm.
#*********************************************************************

import bisect

def bisect_left(sorted_collection, item, lo=0, hi=None):
    
    if hi is None:
        hi = len(sorted_collection)

    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_collection[mid] < item:
            lo = mid + 1
        else:
            hi = mid

    return lo


def bisect_right(sorted_collection, item, lo=0, hi=None):
    
    if hi is None:
        hi = len(sorted_collection)

    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_collection[mid] <= item:
            lo = mid + 1
        else:
            hi = mid

    return lo


def insort_left(sorted_collection, item, lo=0, hi=None):
    
    sorted_collection.insert(bisect_left(sorted_collection, item, lo, hi), item)


def insort_right(sorted_collection, item, lo=0, hi=None):
    
    sorted_collection.insert(bisect_right(sorted_collection, item, lo, hi), item)


def binary_search(sorted_collection, item):
    
    left = 0
    right = len(sorted_collection) - 1

    while left <= right:
        midpoint = left + (right - left) // 2
        current_item = sorted_collection[midpoint]
        if current_item == item:
            return midpoint
        elif item < current_item:
            right = midpoint - 1
        else:
            left = midpoint + 1
    return None


def binary_search_std_lib(sorted_collection, item):
    
    index = bisect.bisect_left(sorted_collection, item)
    if index != len(sorted_collection) and sorted_collection[index] == item:
        return index
    return None


def binary_search_by_recursion(sorted_collection, item, left, right):
     
    if right < left:
        return None

    midpoint = left + (right - left) // 2

    if sorted_collection[midpoint] == item:
        return midpoint
    elif sorted_collection[midpoint] > item:
        return binary_search_by_recursion(sorted_collection, item, left, midpoint - 1)
    else:
        return binary_search_by_recursion(sorted_collection, item, midpoint + 1, right)


def __assert_sorted(collection):
    
    if collection != sorted(collection):
        raise ValueError("Collection must be ascending sorted")
    return True


if __name__ == "__main__":

    collection = [1, 2, 5, 7, 8, 10, 11, 15]
    print("List numbers: %s\n" % repr(collection))

    __assert_sorted(collection)

    target_input = input("Enter a single number to be found in the list:\n")
    target = int(target_input)
    result = binary_search(collection, target)
    
    if result is not None:
        print("%s found at positions: %s" % (target, result))
    else:
        print("Number not found in list")