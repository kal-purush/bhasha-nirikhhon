package LeetCode;

/**
 * https://leetcode.com/problems/search-insert-position/
 *
 * 35. Search Insert Position
 * Easy
 * 12.8K
 * 560
 * Companies
 *
 * Given a sorted array of distinct integers and a target value, return the index if the target is found. If not, return the index where it would be if it were inserted in order.
 *
 * You must write an algorithm with O(log n) runtime complexity.
 *
 *
 *
 * Example 1:
 *
 * Input: nums = [1,3,5,6], target = 5
 * Output: 2
 *
 * Example 2:
 *
 * Input: nums = [1,3,5,6], target = 2
 * Output: 1
 *
 * Example 3:
 *
 * Input: nums = [1,3,5,6], target = 7
 * Output: 4
 *
 *
 *
 * Constraints:
 *
 *     1 <= nums.length <= 104
 *     -104 <= nums[i] <= 104
 *     nums contains distinct values sorted in ascending order.
 *     -104 <= target <= 104
 */
public class _35_Easy_SearchInsertPosition
{
	public static void main(String[] args) throws Exception
	{
		System.out.println(searchInsert(new int[]{1,3,5,6}, 5));
		System.out.println(searchInsert(new int[]{1,3,5,6}, 2));
		System.out.println(searchInsert(new int[]{1,3,5,6}, 7));
	}
	public static int searchInsert(int[] nums, int target)
	{
		int length = nums.length;
		int l = 0, r = length - 1, m;
		while(l < r)
		{
			m = (l + (r - 1)) / 2;
			if(nums[m] == target)
			{
				return m;
			}
			if(nums[m] > target)
			{
				r = m;
			}
			else
			{
				l = m + 1;
			}
		}
		return nums[l] < target ? l + 1 : l;
	}
}