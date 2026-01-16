package Algorithms;

import java.util.Arrays;

public class BinarySearchTest {

    /**
     * 二分查找要求数列有序
     * @param arr
     * @param a
     * @return
     */
    boolean binarySearch(int[] arr,int a) {
        int i = 0;
        int j = arr.length-1;
        while(i <= j) { // Remember that equal sign is necessary.
            int middle = (i+j) >> 1;
            if(a == arr[middle]) {
                return true;
            } else if (a > arr[middle]) {
                i = middle + 1;
            } else {
                j = middle - 1;
            }
        }
        return false;
    }
    public static void main(String[] args) {
        int[] arr = {1,2,3,4,5,6,7,8};
        System.out.println(new BinarySearchTest().binarySearch(arr, 1));
        System.out.println(new BinarySearchTest().binarySearch(arr, 2));
        System.out.println(new BinarySearchTest().binarySearch(arr, 3));
        System.out.println(new BinarySearchTest().binarySearch(arr, 4));
        System.out.println(new BinarySearchTest().binarySearch(arr, 5));
        System.out.println(new BinarySearchTest().binarySearch(arr, 6));
        System.out.println(new BinarySearchTest().binarySearch(arr, 7));
        System.out.println(new BinarySearchTest().binarySearch(arr, 8));
        System.out.println(new BinarySearchTest().binarySearch(arr, 9));
        System.out.println(Arrays.binarySearch(arr, 3));
//		System.out.println((-8)>>1);
//		System.out.println((8+8)>>>1);
    }
}