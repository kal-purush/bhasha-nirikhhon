package com.jay.biz_movie;

/**
 * @author wangxuejie
 * @version 1.0
 * @date 2020/8/16
 */
public class NumberUtils {
    /**
     * 随机指定范围内N个不重复的随机数
     * 最简单最易理解的两重循环去重
     *
     * @param min 指定范围最小值
     * @param max 指定范围最大值
     * @param n   随机数个数
     */
    public static int[] getRandomNumArray(int min, int max, int n) {
        //条件校验
        if (n > (max - min + 1) || max < min) {
            return new int[0];
        }
        int[] result = new int[n];
        //记录随机数的个数
        int count = 0;
        while (count < n) {
            // Math.random() 返回带正号的 double 值，该值的区间为[0.0,1.0)
            // (max=10,min=1,random=0): num=1;
            // (max=10,min=1,random=0.5): num=4;
            int num = (int) (Math.random() * (max - min)) + min;
            // 标记是否有重复元素
            boolean flag = true;
            // 遍历整个数组查找重复项
            for (int j = 0; j < n; j++) {
                if (num == result[j]) {
                    flag = false;
                    break;
                }
            }
            // 当前随机数不包含在数组中，添加进数组
            if (flag) {
                result[count] = num;
                count++;
            }
        }
        return result;
    }

}