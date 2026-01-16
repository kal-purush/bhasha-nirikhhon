package com.ynm.gms.sorting;

/**
 * @author Yogesh Manware
 */
public class QuickSort {

    public void sort(int[] testArr2) {
        sort(testArr2, 0, testArr2.length - 1);
    }

    private void sort(int[] testArr2, int low, int hi) {
        if (!(low < hi)) {
            return;
        }

        int pivotIndex = partition(testArr2, low, hi);
        sort(testArr2, low, pivotIndex - 1);
        sort(testArr2, pivotIndex + 1, hi);
    }

    private int partition(int[] testArr2, int low, int hi) {
        int pivot = testArr2[hi];
        int i = low - 1;
        for (int j = low; j < hi; j++) {
            if (pivot > testArr2[j]) {
                ++i;
                swap(testArr2, i, j);
            }
        }
        swap(testArr2, ++i, hi);
        return i;
    }

    private void swap(int[] testArr2, int i, int j) {
        int temp = testArr2[i];
        testArr2[i] = testArr2[j];
        testArr2[j] = temp;
    }
}