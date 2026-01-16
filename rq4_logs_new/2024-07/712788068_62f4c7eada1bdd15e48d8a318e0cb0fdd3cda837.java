import java.util.*;

/**
 * LeetCode 1480. Running Sum of 1d Array
 * https://leetcode.com/problems/running-sum-of-1d-array/
 */
public class Solution {
  public static void main(String[] args) {
    var a = new int[] { 1, 2, 3, 4 };
    int[] actual = runningSum(a);
    var expected = new int[] { 1, 3, 6, 10 };
    System.out.println((Arrays.equals(actual, expected) ? "🟢" : "🔴")
        + " Input" + Arrays.toString(a) + " -> "
        + " Result " + Arrays.toString(actual)
        + " Expected " + Arrays.toString(expected));
  }

  // Runtime: 0ms Beats 100% | Memory: 42.59 MB Beats 48.14%
  public static int[] runningSum(int[] nums) {
    for (int i = 1; i < nums.length; i++) {
      nums[i] += nums[i - 1];
    }
    return nums;
  }
}