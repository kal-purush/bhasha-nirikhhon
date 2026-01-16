package com.example.demo.leetcode.i;

/**
 * Description: 2367. 算术三元组的数目
 *  给你一个下标从 0 开始、严格递增 的整数数组 nums 和一个正整数 diff 。如果满足下述全部条件，则三元组 (i, j, k) 就是一个 算术三元组 ：
 *
 *  i < j < k ，
 *  nums[j] - nums[i] == diff 且
 *  nums[k] - nums[j] == diff
 *  返回不同 算术三元组 的数目。
 *
 * 来源：力扣（LeetCode）
 * 链接：https://leetcode.cn/problems/number-of-arithmetic-triplets
 *
 * @author Zeti
 * @date 2023/3/31 11:04
 */
public class ArithmeticTriplets {

    // 即求长度为3的等差数组（等差为diff）的个数
    public static void main(String[] args) {
        int[] n1 = {0,1,4,6,7,10};
        int d1 = 3;
        System.err.println(arithmeticTriplets(n1, d1));

        int[] n2 = {4,5,6,7,8,9};
        int d2 = 2;
        System.err.println(arithmeticTriplets(n2, d2));

    }

    // 输入：nums = [0,1,4,6,7,10], diff = 3
    // 输出：2
    // 解释：
    //(1, 2, 4) 是算术三元组：7 - 4 == 3 且 4 - 1 == 3 。
    //(2, 4, 5) 是算术三元组：10 - 7 == 3 且 7 - 4 == 3 。

    //输入：nums = [4,5,6,7,8,9], diff = 2
    //输出：2
    //(0, 2, 4) 是算术三元组：8 - 6 == 2 且 6 - 4 == 2 。
    //(1, 3, 5) 是算术三元组：9 - 7 == 2 且 7 - 5 == 2 。

    // 暴力双循环
    public static int arithmeticTriplets(int[] nums, int diff) {
        int ct = 0;

        for(int i = 0; i < nums.length; i++) {
            int nm = nums[i];
            int tg = 1;
            for(int j = i+1; j< nums.length; j++) {
                if (nums[j] == nm+diff || nums[j] == nm+2*diff) {
                    tg++;
                    if (tg >= 3) {
                        ct++;
                    }
                }
            }
        }
        return ct;
    }

}