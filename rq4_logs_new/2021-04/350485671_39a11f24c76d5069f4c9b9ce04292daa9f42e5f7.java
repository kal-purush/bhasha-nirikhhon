package challenges.sorting;

import java.util.Arrays;

public class MergeSort {
    static public int[] mergeSort(int[] array) {
        int[] result = new int[array.length];
        if (array.length < 4) { // Insertion sort if the array is small. In practice the optimal value is much higher.
            System.arraycopy(array, 0, result, 0, array.length);
            InsertionSort.insertionSort(result);
        } else {
            int[] first = mergeSort(Arrays.copyOfRange(array, 0, array.length / 2));
            int[] second = mergeSort(Arrays.copyOfRange(array, array.length / 2, array.length));

            int i = 0;
            int j = 0;
            int k = 0;
            while (i < first.length || j < second.length)
                result[k++] = i < first.length && (j == second.length || first[i] < second[j])
                        ? first[i++] : second[j++];
        }

        return result;
    }
}