package LeetCode_Medium;

import java.util.Arrays;

public class Find_First_and_Last_Position_of_Element_in_Sorted_Array {

    public static void main(String[] args) {
        int[] arr = {5, 7, 7, 8, 8, 10};
        System.out.println(Arrays.toString(searchRange(arr, 8)));

    }

    public static int[] searchRange(int[] nums, int target) {
        int begin = -1;
        int end = -1;
        int[] outputArr = new int[2];

        if (nums.length == 1) {
            if (nums[0] == target) {
                outputArr[0] = 0;
                outputArr[1] = 0;
                return outputArr;
            } else {
                outputArr[0] = -1;
                outputArr[1] = -1;
                return outputArr;
            }
        }


        if (nums.length == 0) {
            outputArr[0] = -1;
            outputArr[1] = -1;
            return outputArr;
        }

        for (
                int i = 0, j = nums.length - 1;
                i < nums.length; i++, j--) {
            if (begin == -1 && nums[i] == target) {
                begin = i;
            }
            if (end == -1 && nums[j] == target) {
                end = j;
            }

            if (begin != -1 && end != -1) {
                break;
            }
        }


        outputArr[0] = begin;
        outputArr[1] = end;

        return outputArr;
    }
}