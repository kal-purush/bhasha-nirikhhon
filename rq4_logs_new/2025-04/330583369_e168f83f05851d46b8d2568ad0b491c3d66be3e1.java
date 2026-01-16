package com.horadrim.staff.ltcd.array;

public class AlternatingSubarray {
    int solution(int [] array) {
        if (array.length <= 1) {
            return 0;
        }

        int result = 0;
        int cur = 0;
        int flag = -1;
        for (int i = 1; i < array.length; ++i) {
            if (array[i] - array[i - 1] == (int) Math.pow(flag, i - 1)) {
                ++cur;
            } else {
                cur = 0;
            }

            if (cur > result) {
                result = cur;
            }
        }

        return (result == 0 ? result : result + 1);
    }

    int solutionII(int [] nums) {
        int res = 0;
        int n = nums.length;
        // firstIndex为交替子序的基点
        int firstIndex = 0;
        for (int i = 1; i < n; i++) {
            // length用于计算是否满足S1 - S0 = 1，S2 - S1 = -1，同时作为交替子序的长度
            int length = i - firstIndex + 1;

            // S1 - S0 = 1，S2 - S1 = -1可以归结为S1 - S0 = 1, S2 - S0 = 0, S3 - S0 = 1
            if (nums[i] - nums[firstIndex] == (length - 1) % 2) {
                res = Math.max(res, length);
            } else {
                // 处理[2, 3, 4]这样的情况此时作为基点的firstIndex需要更新
                if (nums[i] - nums[i - 1] == 1) {
                    firstIndex = i - 1;
                    res = Math.max(res, 2);
                // 处理[2, 3, 5]这样的情况此时作为基点的firstIndex需要更新
                } else {
                    firstIndex = i;
                }
            }
        }
        return res;
    }
}