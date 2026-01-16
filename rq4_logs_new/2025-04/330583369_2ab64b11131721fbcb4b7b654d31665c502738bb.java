/*
 * 给定一个整数X和一个数组arr，要求将数组分成三个部分，
 * 所有小于X的元素放在第一部分，等于X的放在第二部分，大于
 * X的放在第三部分，每个部分内的顺序可以是任意的，但需要
 * 保证这三个部分位置的相对正确。
 */

package com.horadrim.staff.ltcd.array;

import java.util.Arrays;

public class ArrayPartition {
    public int [] solution(int k, int [] input) {
        if (input.length == 0) {
            return input;
        }
        int [] output = Arrays.copyOf(input, input.length);
        int left = 0, middle = 0, right = input.length - 1;
        while (middle <= right) {
            if (output[middle] < k) {
                int t = output[left];
                output[left] = output[middle];
                output[middle] = t;
                ++left;
                ++middle;
            } else if (output[middle] == k) {
                ++middle;
            } else {
                int t = output[middle];
                output[middle] = output[right];
                output[right] = t;
                --right;
            }
        }

        return output;
    }
}