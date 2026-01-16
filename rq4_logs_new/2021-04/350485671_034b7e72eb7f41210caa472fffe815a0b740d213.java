package challenges.sorting;

import java.util.List;

public class HeapSort {
    /**
     * Sorts a list of ints in place in O(n log n) time.
     * @param array An array of ints that will be sorted
     */
    static void heapSort(int[] array) {
        for (int i = array.length / 2 - 1; i >= 0; i--)
            heapifyDown(array, i, array.length);

        for (int i = 0; i < array.length - 1; i++) {
            int temp = array[0];
            array[0] = array[array.length - 1 - i];
            array[array.length - 1 - i] = temp;

            heapifyDown(array, 0, array.length - 1 - i);
        }
    }

    static private void heapifyDown (int[] arr, int j, int l) {
        while (2 * j + 1 < l) {
            int k = 2 * j + 2 < l && arr[2 * j + 2] > arr[2 * j + 1] ? 2 * j + 2 : 2 * j + 1;
            if (arr[j] < arr[k]) {
                int temp = arr[k];
                arr[k] = arr[j];
                arr[j] = temp;
                j = k;
            } else break;
        }
    };

}