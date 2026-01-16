package mytest.algorithm.doublepoint;

/**
 * //给你 n 个非负整数 a1，a2，...，an，每个数代表坐标中的一个点 (i, ai) 。在坐标内画 n 条垂直线，垂直线 i 的两个端点分别为 (i,
 * //ai) 和 (i, 0) 。找出其中的两条线，使得它们与 x 轴共同构成的容器可以容纳最多的水。
 * //
 * // 说明：你不能倾斜容器。
 * // 示例 1：
 * //
 * //输入：[1,8,6,2,5,4,8,3,7]
 * //输出：49
 * //解释：图中垂直线代表输入数组 [1,8,6,2,5,4,8,3,7]。在此情况下，容器能够容纳水（表示为蓝色部分）的最大值为 49。
 * //
 * // 示例 2：
 * //
 * //
 * //输入：height = [1,1]
 * //输出：1
 * //
 * //
 * // 示例 3：
 * //
 * //
 * //输入：height = [4,3,2,1,4]
 * //输出：16
 * //
 * //
 * // 示例 4：
 * //
 * //
 * //输入：height = [1,2,1]
 * //输出：2
 * // 提示：
 * //
 * //
 * // n = height.length
 * // 2 <= n <= 3 * 104
 * // 0 <= height[i] <= 3 * 104
 * //
 * // Related Topics 数组 双指针
 * https://leetcode-cn.com/problems/container-with-most-water/
 *
 * @author : yufei
 * @date : 2021/1/15 16:14
 * @description :
 */
public class MaxArea {
    // 从两端开始，向中间遍历，水槽的高度由最短的确定
    // 如果固定最端的一边，向固定端移动另一边，水通一定不会比之前多
    public int maxArea(int[] height) {
        if (height == null || height.length < 2) {
            return 0;
        }
        int max = 0;
        int left = 0;
        int right = height.length - 1;
        while (left < right) {
            int curArea = Math.min(height[left], height[right]) * (right - left);
            max = Math.max(curArea, max);
            // 移动较小的那一边，因为移动最大的那边，得到的结果肯定不会比当前大
            if (height[left] < height[right]) {
                left++;
            } else {
                right--;
            }
        }

        return max;
    }
}