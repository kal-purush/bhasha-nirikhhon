package challenges.sorting;

public class QuickSort {
    /**
     * Sorts a list of ints in place in O(n log n) average time, and O(n^2) worst case time.
     *
     * @param array An array of ints that will be sorted
     */
    static void quickSort(int[] array) {
        quickSort(array, 0, array.length);
    }

    /**
     * Sorts a sub-array of [array] in place between the indices [start] (inclusive) and [end] (exclusive)
     */
    static private void quickSort(int[] array, int start, int end) {
        if (start + 1 < end) {
            int pivotIndex = quickSortPartition(array, start, end);
            quickSort(array, start, pivotIndex);
            quickSort(array, pivotIndex + 1, end);
        }
    }

    static public void quickSortWithInsertion(int[] array) {
        quickSortWithInsertion(array, 0, array.length);
    }
    /**
     * Moves all the elements less than the pivot element to the beginning of an array, and all the elements greater
     * than a given element to the end of the array, and returns the index of the pivot element
     */

    static private int quickSortPartition(int[] array, int start, int end) {
        int pivot = array[end - 1];
        int low = start;

        for (int i = start; i < end; i++)
            if (array[i] <= pivot) swap(array, i, low++);

        return low - 1;
    }

    /**
     * Sorts a sub-array of [array] in place between the indices [start] (inclusive) and [end] (exclusive)
     */
    static private void quickSortWithInsertion(int[] array, int start, int end) {
        // In practice we'd perform insertion sort when the size of the sub-array is less than some threshold.
        int THRESHOLD = 10;

        if (start + THRESHOLD >= end) {
            InsertionSort.insertionSort(array, start, end);
        } else {
            int pivotIndex = quickSortPartition(array, start, end);
            quickSort(array, start, pivotIndex);
            quickSort(array, pivotIndex + 1, end);
        }
    }


    static private void swap(int[] array, int i, int j) {
        int temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
}