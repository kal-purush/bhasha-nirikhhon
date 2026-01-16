package sorts;

import java.util.ArrayList;

/**
 *
 * @author canto5684
 */
public class Sorts {

    /**
     * Sorts an array of integer from low to high pre: none post: Integers have
     * been sorted from low to high
     */
    public static void selectionSort(int[] items) {
        for (int index = 0; index < items.length; index++) {
            for (int subIndex = index + 1; subIndex < items.length; subIndex++) {
                if (items[subIndex] < items[index]) {
                    int temp = items[index];
                    items[index] = items[subIndex];
                    items[subIndex] = temp;
                }
            }
        }
    }

    /**
     * Sorts an array of Comparable items from low to high pre: none post: Items
     * have been sorted from low to high
     */
    public static void selectionSort(Comparable[] items) {
        for (int index = 0; index < items.length; index++) {
            for (int subIndex = index + 1; subIndex < items.length; subIndex++) {
                if (items[subIndex].compareTo(items[index]) == -1) {
                    Comparable temp = items[index];
                    items[index] = items[subIndex];
                    items[subIndex] = temp;
                }
            }
        }
    }

    /**
     * Sorts an ArrayList of Comparable items from low to high pre: none post:
     * Items have been sorted from low to high
     */
    public static void selectionSort(ArrayList<Comparable> items) { //NOT FINISHED
        for (int index = 0; index < items.size(); index++) {
            for (int subIndex = index + 1; subIndex < items.size(); subIndex++) {
                if (items.get(subIndex).compareTo(items.get(index)) == -1) {
                    Comparable temp = items.get(index);
                    items.set(index, items.get(subIndex));
                    items.set(subIndex, temp);
                }
            }
        }
    }

    /**
     * Sorts an array of integer from low to high pre: none post: Integers have
     * been sorted from low to high
     */
    public static void insertionSort(int[] items) {
        int temp, previousIndex;

        for (int index = 1; index < items.length; index++) {
            temp = items[index];
            previousIndex = index - 1;
            while ((items[previousIndex] > temp) && (previousIndex > 0)) {
                items[previousIndex + 1] = items[previousIndex];
                previousIndex -= 1; //decrease index to compare current
                //item with next previous item
            }
            if (items[previousIndex] > temp) {
                /* shift item in first element up into next element */
                items[previousIndex + 1] = items[previousIndex];
                /* place current item at index 0 (first element) */
                items[previousIndex] = temp;
            } else {
                /* place current item at index ahead of previous item */
                items[previousIndex + 1] = temp;
            }
        }
    }

    /**
     * Sorts an array of integer from low to high pre: none post: Integers have
     * been sorted from low to high
     */
    public static void insertionSort(Comparable[] items) {
        Comparable temp;
        int previousIndex;

        for (int index = 1; index < items.length; index++) {
            temp = items[index];
            previousIndex = index - 1;
            while ((items[previousIndex].compareTo(temp) == 1) && (previousIndex > 0)) {
                items[previousIndex + 1] = items[previousIndex];
                previousIndex -= 1; //decrease index to compare current
                //item with next previous item
            }
            if (items[previousIndex].compareTo(temp) == 1) {
                /* shift item in first element up into next element */
                items[previousIndex + 1] = items[previousIndex];
                /* place current item at index 0 (first element) */
                items[previousIndex] = temp;
            } else {
                /* place current item at index ahead of previous item */
                items[previousIndex + 1] = temp;
            }
        }
    }

    /**
     * Sorts an array of integer from low to high pre: none post: Integers have
     * been sorted from low to high
     */
    public static void insertionSort(ArrayList<Comparable> items) {
        Comparable temp;
        int previousIndex;

        for (int index = 1; index < items.size(); index++) {
            temp = items.get(index);
            previousIndex = index - 1;
            while ((items.get(previousIndex).compareTo(temp) == 1) && (previousIndex > 0)) {
                items.set(previousIndex + 1, items.get(previousIndex));
                previousIndex -= 1; //decrease index to compare current
                //item with next previous item
            }
            if (items.get(previousIndex).compareTo(temp) == 1) {
                /* shift item in first element up into next element */
                items.set(previousIndex + 1, items.get(previousIndex));
                /* place current item at index 0 (first element) */
                items.set(previousIndex, temp);
            } else {
                /* place current item at index ahead of previous item */
                items.set(previousIndex + 1, temp);
            }
        }
    }

    /**
     * Sorts items[start..end] pre: start > 0, end > 0 post: items[start..end]
     * is sorted low to high
     */
    public static void mergesort(int[] items, int start, int end) {
        if (start < end) {
            int mid = (start + end) / 2;
            mergesort(items, start, mid);
            mergesort(items, mid + 1, end);
            merge(items, start, mid, end);
        }
    }

    /**
     * Merges two sorted portion of items array pre: items[start..mid] is
     * sorted. items[mid+1..end] sorted. start <= mid <= end post:
     * items[start..end] is sorted.
     */
    private static void merge(int[] items, int start, int mid, int end) {
        int[] temp = new int[items.length];
        int pos1 = start;
        int pos2 = mid + 1;
        int spot = start;
        while (!(pos1 > mid && pos2 > end)) {
            if ((pos1 > mid) || ((pos2 <= end) && (items[pos2] < items[pos1]))) {
                temp[spot] = items[pos2];
                pos2 += 1;
            } else {
                temp[spot] = items[pos1];
                pos1 += 1;
            }
            spot += 1;
        }
        /* copy values from temp back to items */
        for (int i = start; i <= end; i++) {
            items[i] = temp[i];
        }
    }

    /**
     * Sorts items[start..end] pre: start > 0, end > 0 post: items[start..end]
     * is sorted low to high
     */
    public static void mergesort(Comparable[] items, int start, int end) {
        if (start < end) {
            int mid = (start + end) / 2;
            mergesort(items, start, mid);
            mergesort(items, mid + 1, end);
            merge(items, start, mid, end);
        }
    }

    /**
     * Merges two sorted portion of items array pre: items[start..mid] is
     * sorted. items[mid+1..end] sorted. start <= mid <= end post:
     * items[start..end] is sorted.
     */
    private static void merge(Comparable[] items, int start, int mid, int end) {
        Comparable[] temp = new Comparable[items.length];
        int pos1 = start;
        int pos2 = mid + 1;
        int spot = start;
        while (!(pos1 > mid && pos2 > end)) {
            if ((pos1 > mid) || ((pos2 <= end) && (items[pos2].compareTo(items[pos1]) == -1))) {
                temp[spot] = items[pos2];
                pos2 += 1;
            } else {
                temp[spot] = items[pos1];
                pos1 += 1;
            }
            spot += 1;
        }
        /* copy values from temp back to items */
        for (int i = start; i <= end; i++) {
            items[i] = temp[i];
        }
    }

    /**
     * Sorts items[start..end] pre: start > 0, end > 0 post: items[start..end]
     * is sorted low to high
     */
    public static void mergesort(ArrayList<Comparable> items, int start, int end) {
        if (start < end) {
            int mid = (start + end) / 2;
            mergesort(items, start, mid);
            mergesort(items, mid + 1, end);
            merge(items, start, mid, end);
        }
    }

    /**
     * Merges two sorted portion of items array pre: items[start..mid] is
     * sorted. items[mid+1..end] sorted. start <= mid <= end post:
     * items[start..end] is sorted.
     */
    private static void merge(ArrayList<Comparable> items, int start, int mid, int end) {
        Comparable[] temp = new Comparable[items.size()];
        int pos1 = start;
        int pos2 = mid + 1;
        int spot = start;
        while (!(pos1 > mid && pos2 > end)) {
            if ((pos1 > mid) || ((pos2 <= end) && (items.get(pos2).compareTo(items.get(pos1)) == -1))) {
                temp[spot] = items.get(pos2);
                pos2 += 1;
            } else {
                temp[spot] = items.get(pos1);
                pos1 += 1;
            }
            spot += 1;
        }
        /* copy values from temp back to items */
        for (int i = start; i <= end; i++) {
            items.set(i, temp[i]);
        }
    }

    
}