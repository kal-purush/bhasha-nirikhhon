package LeetCode;

import java.util.HashMap;
import java.util.Map;

/**
 * https://leetcode.com/problems/two-sum/
 * <p>
 * Given an array of integers nums and an integer target,
 * return indices of the two numbers such that they add up to target.
 * <p>
 * You may assume that each input would have exactly one solution,
 * and you may not use the same element twice.
 * <p>
 * You can return the answer in any order.
 * <p>
 * <p>
 * <p>
 * Example 1:
 * <p>
 * Input: nums = [2,7,11,15], target = 9
 * Output: [0,1]
 * Explanation: Because nums[0] + nums[1] == 9, we return [0, 1].
 * <p>
 * Example 2:
 * <p>
 * Input: nums = [3,2,4], target = 6
 * Output: [1,2]
 * <p>
 * Example 3:
 * <p>
 * Input: nums = [3,3], target = 6
 * Output: [0,1]
 * <p>
 * <p>
 * <p>
 * Constraints:
 * <p>
 * 2 <= nums.length <= 104
 * -109 <= nums[i] <= 109
 * -109 <= target <= 109
 * Only one valid answer exists.
 * <p>
 * <p>
 * Follow-up: Can you come up with an algorithm that is less than O(n2) time complexity?
 */

public class _1_TwoSum
{
	public int[] twoSum(int[] nums, int target)
	{
		Map<Integer, Integer> map = new HashMap<>();
		for(int i = 0; i < nums.length; i++)
		{
			// Time Complexity : O(n) as we iterate only once the array of numbers
			// Below approach reduces time complexity to O(n) from O(n2) which resulted in Brute force approach
			//Space Complexity : O(n) as we use an extra HashMap to store the n elements from the nums[] array.
			int balance = target - nums[i];
			if(map.containsKey(balance))
			{
				return new int[] {map.get(balance), i};
			}
			map.put(nums[i], i);

			// Below is the Brute force approach of the same
			// Time Complexity : O(n2)
			// Space Complexity : O(1)
			// for(int j=i+1;j<nums.length;j++)
			// {
			//     if(nums[i]+nums[j]==target)
			//     {
			//         return new int[]{i, j};
			//     }
			// }
		}
		return null;
	}
}