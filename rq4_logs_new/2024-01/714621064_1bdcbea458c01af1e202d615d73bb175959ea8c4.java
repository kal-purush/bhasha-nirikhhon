package LeetCode_Easy;

import java.util.Arrays;

public class Missing_Number {
    public static void main(String[] args) {

        int[] nums = {3, 0, 1};
        System.out.println(missingNumber(nums));

    }

    public static int missingNumber(int[] nums) {
        Arrays.sort(nums);
        int toCheck = nums[0];

        if (nums[0] > 0) {
            return 0;
        }

        for (int i = 0; i < nums.length; i++) {
            if (toCheck != nums[i]) {
                return toCheck;
            }
            toCheck++;
        }

        return nums[nums.length - 1] + 1;
    }
}