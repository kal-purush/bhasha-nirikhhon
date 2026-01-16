package com.fgh.alg.classic;

import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fgh
 * @since 2019/4/2 23:40
 */
public class TwoSum {


    /**
     * ʱ�临�Ӷ� O(N^2)
     *
     * @param nums
     * @param target
     * @return
     */
    public int[] getTwo1(int[] nums, int target) {
        int[] result = new int[2];
        for (int i = 0; i < nums.length; i++) {
            int a = nums[i];
            for (int j = nums.length - 1; j >= 0; j--) {
                int b = nums[j];
                if ((a + b) == target) {
                    result = new int[]{i, j};
                }
            }
        }
        return result;

    }

    /**
     * ʱ�临�Ӷ�O(N)
     * @param nums
     * @param target
     * @return
     */
    private static int[] twoSum(int[] nums, int target) {
        int[] result = new int[2];
        Map<Integer, Integer> map = new HashMap();
        for (int i = 0; i < nums.length; i++) {
            if (map.containsKey(nums[i])) {
                result = new int[]{map.get(nums[i]), i};
            }
            map.put(target - nums[i], i);
        }
        System.out.println("������ת��map��map=" + map);

//        for (int i = 0; i < nums.length; i++) {
//            if (map.containsKey(result) && map.get(result) != i) {
//                return new int[]{nums[i], i};
//            }
//        }
        throw new IllegalArgumentException("No two sum solution");
    }

    public static void main(String[] args) {
        int[] arr = new int[]{2, 7, 11, 15};
        int target = 9;
        int[] indexArr = twoSum(arr, target);

        for (int i : indexArr) {
            System.out.println(i);
        }
    }
}