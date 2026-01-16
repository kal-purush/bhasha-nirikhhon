package org.example.homework6;

import java.util.Arrays;
public class ArrayReverse {
    public static void main(String[] args) {
        int[] array = {1, 33, 55, 77, 88, 3};
        reverseArray(array);
        System.out.println(Arrays.toString(array));
    }
    private static void reverseArray(int[] array) {
        int size = array.length;
        for (int i = 0; i < size/2; i++) {
            int temp = array[i];
            array[i] = array[size - 1 - i];
            array[size - 1 - i] = temp;
        }
    }
}