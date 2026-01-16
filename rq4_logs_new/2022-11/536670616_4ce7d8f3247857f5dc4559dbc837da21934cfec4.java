package Sorting;

// https://leetcode.com/problems/merge-sorted-array/

import java.util.Arrays;

public class MergeSortedArray {
    public static void main(String[] args) {
        int[] nums1 = {2,0};
        int[] nums2 = {1};
        merge(nums1,1, nums2,1);
        System.out.println(Arrays.toString(nums1));
    }

    public static void merge(int[] nums1, int m, int[] nums2, int n) {

        int i = 0;
        int j = 0;

        if(m==1 && nums1[0] == 0){
            nums1[i] = nums2[j];
        } else if(m==1 && n==0){
            return;
        }
        else {
            for (i = m; i < nums1.length ; i++) {
                nums1[i] = nums2[j++];
            }
        }
        for(i=0; i< nums1.length;i++){
            int last = nums1.length-1-i;
            int max = getMaxIndex(nums1,0,last);
            swap(nums1,max,last);
        }
    }

    private static int getMaxIndex(int[] nums1, int start, int last) {
        int max = start;
        for(int i=start;i<=last;i++){
            if(nums1[max]<nums1[i])
                max = i;
        }
        return max;
    }

    static void swap(int[] nums1, int i, int j){
        int temp = nums1[i];
        nums1[i] = nums1[j];
        nums1[j] = temp;
    }

}


