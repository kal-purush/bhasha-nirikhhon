package com.yearup.week4.code.sorting;

import com.yearup.*;
import java.util.Arrays;
import com.yearup.week4.code.sorting.*;

public class Demo {

    static long startTime;
    static long endTime;
    static long elapsedTime;





    public static int linearSearch(int[] arr, int key){
        for(int i=0;i<arr.length;i++){
            if(arr[i] == key){
                return i;
            }
        }
        return -1;
    }

    public static int binarySearch2(int arr[], int num)
    {
        Arrays.sort(arr); //Array must be sorted if not sorted already for Binary Search

        System.out.println("Sorted Array: "+Arrays.toString(arr));
        int l = 0;
        int h = arr.length - 1;

        while (l <= h) {
            int idx = (l + h) / 2;
            if (num > arr[idx]) {
                l = idx + 1;
            } else if (num < arr[idx]) {
                h = idx - 1;
            } else {
                System.out.println(idx);
                return idx;
            }
        }
        return -1;
    }

    public static void swap(int[] arr, int i, int j) {

    }

    public static boolean isSmaller(int[] arr, int i, int j) {

        return true;

    }


    public static void main(String args[])
    {
        int index;
        //Linear Search on an Unsorted Array
        int[] arr1 = {56,12,38,23,2,8,72,91,5,16};
        index=linearSearch(arr1,16);
        System.out.println("Element found in Unsorted Array at index: "+index);
        startTime =  System.nanoTime();
        endTime =  System.nanoTime();
        elapsedTime = endTime-startTime;
        System.out.println("Total Elapsed Time for Linear Search is: "+elapsedTime);

        //Binary Search on a Sorted Array
        int[] arr2 = {56,12,38,23,2,8,72,91,5,16};
        int index2=binarySearch2(arr2,91);
        System.out.println("Element found in Array at index: "+index2);
        startTime =  System.nanoTime();
        endTime =  System.nanoTime();
        elapsedTime = endTime-startTime;
        System.out.println("Total Elapsed Time for Binary Search is: "+elapsedTime);

        //Ex:1 Swap Array Elements
        int arr3[] = {5,9,8,2,1};
        System.out.println("Array before swapping elements : "+Arrays.toString(arr3));
        swap(arr3, 4, 1);
        System.out.println("Array after swapping elements : "+Arrays.toString(arr3));

        //Ex:2 Check if Array Element at an index is smaller than the one at another index.
        boolean isSmaller = isSmaller(arr3, 0, 2);
        System.out.println("Array element at index 0 is smaller than the one at index 2 : "+isSmaller);
    }
}