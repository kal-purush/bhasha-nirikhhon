import org.junit.jupiter.api.Test;

import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
/*
 * @Description
 * 最大数
 * 给定一组非负整数 nums，重新排列每个数的顺序（每个数不可拆分）使之组成一个最大的整数。
 * 注意：输出结果可能非常大，所以你需要返回一个字符串而不是整数。
 *
 * 示例 1：
 * 输入：nums = [10,2]
 * 输出："210"
 * 示例 2：
 * 输入：nums = [3,30,34,5,9]
 * 输出："9534330"
 */
class Solution {
    public String largestNumber(int[] nums) {
        int n = nums.length;
        // 转换成包装类型，以便传入 Comparator 对象（此处为 lambda 表达式）
        Integer[] numsArr = new Integer[n];
        for (int i = 0; i < n; i++) {
            numsArr[i] = nums[i];
        }

        Arrays.sort(numsArr, (x, y) -> {
            long sx = 10, sy = 10;
            while (sx <= x) {
                sx *= 10;
            }
            while (sy <= y) {
                sy *= 10;
            }
            return (int) (-sy * x - y + sx * y + x);
        });

        if (numsArr[0] == 0) {
            return "0";
        }
        StringBuilder ret = new StringBuilder();
        for (int num : numsArr) {
            ret.append(num);
        }
        return ret.toString();
    }
}
    class L2022111897_16_Test {

    @Test
    public void testLargestNumber() {
        Solution solution = new Solution();

        // 测试用例 1
        int[] nums1 = {10, 2};
        String expected1 = "210";
        assertEquals(expected1, solution.largestNumber(nums1));

        // 测试用例 2
        int[] nums2 = {3, 30, 34, 5, 9};
        String expected2 = "9534330";
        assertEquals(expected2, solution.largestNumber(nums2));

        // 测试用例 3 - 所有元素为 0
        int[] nums3 = {0, 0};
        String expected3 = "0";
        assertEquals(expected3, solution.largestNumber(nums3));

        // 测试用例 4 - 单个数字
        int[] nums4 = {1};
        String expected4 = "1";
        assertEquals(expected4, solution.largestNumber(nums4));

        // 测试用例 5 - 两个数字组成回文
        int[] nums5 = {12, 121};
        String expected5 = "12121";
        assertEquals(expected5, solution.largestNumber(nums5));
    }
}