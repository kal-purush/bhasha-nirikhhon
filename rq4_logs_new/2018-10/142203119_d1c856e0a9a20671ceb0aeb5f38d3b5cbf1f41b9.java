package org.milan.datastructure;

import java.util.Arrays;

/**
 * Exponential Search - Time Complexity O(logn)
 *
 * @author Milan Rathod
 */
public class ExponentialSearch {

    // Returns position of first occurrence of
    // x in array

    /**
     * @param inputArray given array
     * @param number     element to search
     * @return -1 if not found or index of element if found
     */
    public int exponentialSearch(int[] inputArray, int number) {

        int arraySize = inputArray.length;

        // If x is present at first location itself
        if (inputArray[0] == number)
            return 0;

        // Find range for binary search by
        // repeated doubling
        int i = 1;
        while (i < arraySize && inputArray[i] <= number)
            i = i * 2;

        // Call binary search for the found range.
        return Arrays.binarySearch(inputArray, i / 2,
                Math.min(i, arraySize), number);
    }
}