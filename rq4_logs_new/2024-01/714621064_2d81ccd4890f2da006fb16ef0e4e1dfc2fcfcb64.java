package LeetCode_Easy;

import java.util.Arrays;

public class Merge_Sorted_Array {
    public static void main(String[] args) {

        int[] arr1 = {1,2,3,0,0,0};
        int[] arr2 = {2,5,6};

        merge(arr1, 3, arr2, 3);

    }

    public static void merge(int[] nums1, int m, int[] nums2, int n) {
        if (n != 0) {
            int i = 0;
            while (i < n) {
                nums1[m] = nums2[i];
                i++;
                m++;
            }

            Arrays.sort(nums1);
            System.out.println(Arrays.toString(nums1));
        }
    }
}