package array.easy;

import java.util.ArrayList;
import java.util.List;

public class FindingIntersectionBetweenArrays {
    public static void main(String[] args) {
        // finding the intersection between arrays
        int[] arr1 = {1, 1, 1, 4, 5, 5};
        int[] arr2 = {1, 2, 3, 4, 5, 5};
        System.out.println(intersection(arr1, arr2));


    }

    // two pointer approach
    // time complexity -> O(min(m, n))
    // if one of the array is exhausted, we return
    private static List<Integer> intersection(int[] arr1, int[] arr2) {
        int m = arr1.length;
        int n = arr2.length;
        int i = 0;
        int j = 0;
        List<Integer> list = new ArrayList<>();

        while (i < m && j < n) {
            if (arr1[i] == arr2[j]) {
                list.add(arr1[i]);
                i++;
                j++;
            } else if (arr1[i] < arr2[j]) {
                i++;
            } else {
                j++;
            }
        }
        return list;
    }
}