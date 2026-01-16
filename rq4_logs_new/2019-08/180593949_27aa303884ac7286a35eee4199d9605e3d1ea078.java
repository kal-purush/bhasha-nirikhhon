package com.youcii.algorithm;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

/**
 * 复习较为基础/较为重要的算法
 */
public class Class2 {

    public static void main(String[] args) {

    }

    /**
     * 查找:
     * <p>
     * 二分查找
     */
    @Contract(pure = true)
    private static int binarySearch() {
        return 0;
    }

    /**
     * 排序:
     * <p>
     * 最简单的冒泡排序
     * 时间复杂度: O(n²)
     * 空间复杂度: O(1)
     */
    @NotNull
    @Contract(pure = true)
    private static String bubbleSort() {
        return "";
    }

    /**
     * 排序:
     * <p>
     * 快速排序
     * 时间复杂度: O(n*logn) -> O(n²)
     * 空间复杂度: O(logn)
     * 思路: 分治法
     */
    @NotNull
    @Contract(pure = true)
    private static String fastSort() {
        return "";
    }

    /**
     * 排序:
     * <p>
     * 归并排序
     * 时间复杂度: 稳定O(n*logn)
     * 空间复杂度: O(n)
     * 思路: 分治法
     */
    @NotNull
    @Contract(pure = true)
    private static String mergeSort() {
        return "";
    }

}