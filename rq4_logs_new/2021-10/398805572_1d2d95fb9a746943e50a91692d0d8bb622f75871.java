package com.leetcode.daily.y2021.m10;

import java.util.Arrays;

class d30_Q260 {

    public int[] singleNumber(int[] nums) {
        int xor0 = 0, xor1 = 0;
        for (int i = 0; i <= 31; i++) {
            xor0 = 0; xor1 = 0;
            for (int num : nums) {
                if ((num & (1 << i)) == 0) {
                    xor0 ^= num;
                } else {
                    xor1 ^= num;
                }
            }
            if (xor0 != 0 && xor1 != 0)
                break;
        }
        return new int[]{xor0, xor1};
    }

    public static void main(String[] args) {
        int[] nums = {1, 2, 3, 2, 1, 5};
        int[] res = new d30_Q260().singleNumber(nums);
        System.out.println(Arrays.toString(res));
    }

}