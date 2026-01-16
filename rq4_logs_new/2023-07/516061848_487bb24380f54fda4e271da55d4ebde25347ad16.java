package Easy._414_Third_Maximum_Number;

import java.util.Arrays;

public class Solution {

    public static int thirdMax(int[] nums) {
        int thirdMax = 0;


        if (nums.length == 1) {
            return thirdMax = nums[0];
        }

        if (nums.length == 2) {
            if (nums[0] > nums[1]) {
                return thirdMax = nums[0];
            } else return thirdMax = nums[1];
        }


        int temp = 0;
        for (int i = 0; i < nums.length - 1; i++) {
            for (int j = 0; j < nums.length - i - 1; j++) {
                if (nums[j] > nums[j + 1]) {
                    temp = nums[j];
                    nums[j] = nums[j + 1];
                    nums[j + 1] = temp;
                }
            }
        }


        if(nums.length==3) {
            return nums[0];
        }

        thirdMax = nums.length - 3;

        return thirdMax;

    }

    public static void main(String[] args) {
        int[] array = {1,1,2};

        System.out.println(  thirdMax(array));
    }


}