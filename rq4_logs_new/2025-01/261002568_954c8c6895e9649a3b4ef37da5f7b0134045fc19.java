package org.kaidzen.study.algoexpert.middle;

import java.util.Arrays;

/**
 * To perform effective processing - two arrays are sorted
 * T - O(N*log(N) + M*log(M))
 * S - O(1)
 * with sorted arrays - easy search with array's indexes, according to relations ("<" or ">")
 */
public class SmallestDiff {

    public static void main(String[] args) {
        int[] arr1 = new int[] {-1, 5, 10, 20, 28, 3};
        int[] arr2 = new int[] {26, 134, 135, 15, 17};
        int[] res = SmallestDiff.smallestDifference(arr1, arr2); // [28,26]

        System.out.println(Arrays.toString(res));

    }

    public static int[] smallestDifference(int[] arrayOne, int[] arrayTwo) {
        int[] result = new int[2];
        Arrays.sort(arrayOne);
        Arrays.sort(arrayTwo);
        int idx1 = 0;
        int idx2 = 0;
        int minDiff = Integer.MAX_VALUE;
        int current = Integer.MAX_VALUE;
        while (idx1 < arrayOne.length && idx2 < arrayTwo.length) {
            int first = arrayOne[idx1];
            int second = arrayTwo[idx2];
            if (first < second) {
                current = second - first;
                idx1++;
            } else if (second < first) {
                current = first - second;
                idx2++;
            } else {
                result[0] = first;
                result[1] = second;

                return result;
            }
            if (minDiff > current) {
                minDiff = current;
                result[0] = first;
                result[1] = second;
            }
        }

        return result;
    }
}