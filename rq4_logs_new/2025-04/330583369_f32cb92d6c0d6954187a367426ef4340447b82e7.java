package com.horadrim.staff.ltcd.array;

public class JumpGame {
    /*
     * 贪心算法
     */
    boolean solution(int [] nums) {
        // 从最左边起最远可抵达长度，初始值为0
        int rightMost = 0;
        for (int i = 0; i < nums.length; ++i) {
            // i <= rightMost表示当前可抵达位置i
            if (i <= rightMost) {
                // 更新最远可抵达长度
                rightMost = Math.max(rightMost, i + nums[i]);
                // 当rightMost大于数组长度-1，则能够抵达最右端
                if (rightMost >= nums.length - 1) {
                    return true;
                }
            }
        }

        return false;
    }
}