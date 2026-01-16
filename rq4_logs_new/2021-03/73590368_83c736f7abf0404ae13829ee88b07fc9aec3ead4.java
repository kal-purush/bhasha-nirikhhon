package org.dgf.array;

public class MontonicOrder {
    public static boolean checkOrder(int[] array, boolean direction, int start) {
        for (int i = start; (i+1) < array.length; i++) {
            while ((i+1) < array.length && array[i] == array[i+1]) {
                i++;
            }
            if ((i+1) < array.length) {
                if (direction && array[i] > array[i+1]) {
                    return false;
                }
                if (!direction && array[i] < array[i+1]) {
                    return false;
                }
            }
        }
        return true;
    }


    public static boolean isMonotonic(int[] array) {
        // Write your code here.
        if (array.length <= 2) {
            return true;
        }

        boolean increaseOrder = true;
        int start = 0, equalSize = 0;
        while ((start+1) < array.length && array[start] == array[start+1]) {
            equalSize++;
            start++;
        }
        if (equalSize == (array.length-1)) {
            return true;
        }

        if (array[start] > array[start+1]) {
            increaseOrder = false;
        }

        return checkOrder(array, increaseOrder, start+1);
    }
}