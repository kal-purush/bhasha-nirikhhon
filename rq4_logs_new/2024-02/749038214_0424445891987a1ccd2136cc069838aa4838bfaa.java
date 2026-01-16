package array.easy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FindUnionBetweenArrays {

    public static void main(String[] args) {
        int[] arr1 = {1, 2, 3, 4, 5};
        int[] arr2 = {5, 5, 7, 8, 9};
        System.out.println(findUnion2(arr1, arr2));

    }


    // given two arrays, find the union between two arrays
    // brute force approach
    // using hash set to insert all the numbers
    // from the two given arrays to the set
    // and finally return the set as a list
    //
    private static List<Integer> findUnion(int[] arr1, int[] arr2) {
        Set<Integer> set = new HashSet<>();

        for (int number : arr1) {
            set.add(number);
        }
        for (int number : arr2) {
            set.add(number);
        }

        return new ArrayList<>(set);
    }

    // time complexity -> if all the numbers are distinct then
    // at very least we will iterate through O(m + n) where m -> size of array 1
    // n -> size of array 2

    // optimized solution
    private static List<Integer> findUnion2(int[] arr1, int[] arr2) {
        List<Integer> list = new ArrayList<>();
        int i = 0;
        int j = 0;
        int m = arr1.length;
        int n = arr2.length;
        int size = list.size();

        while (i < m && j < n) {
            if (arr1[i] <= arr2[j]) {
                // check if it is already present in the list
                if (size == 0 || list.get(size - 1) != arr1[i]) {
                    list.add(arr1[i]);
                }
                i++;
            } else {
                if (size == 0 || list.get(size - 1) != arr2[j]) {
                    list.add(arr2[j]);
                }
                j++;
            }
        }
        while (i < m) {
            if (size == 0 || list.get(size - 1) != arr1[i]) {
                list.add(arr1[i]);
            }
            i++;
        }

        while (j < n) {
            if (size == 0 || list.get(size - 1) != arr2[j]) {
                list.add(arr2[j]);
            }
            j++;
        }
        return list;

    }

}