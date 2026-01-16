package LeetCode;

/**
 * https://leetcode.com/problems/median-of-two-sorted-arrays/
 * 
 * 4. Median of Two Sorted Arrays
 * Hard
 *
 * Given two sorted arrays nums1 and nums2 of size m and n respectively,
 * return the median of the two sorted arrays.
 *
 * The overall run time complexity should be O(log (m+n)).
 *
 *
 *
 * Example 1:
 *
 * Input: nums1 = [1,3], nums2 = [2]
 * Output: 2.00000
 * Explanation: merged array = [1,2,3] and median is 2.
 *
 * Example 2:
 *
 * Input: nums1 = [1,2], nums2 = [3,4]
 * Output: 2.50000
 * Explanation: merged array = [1,2,3,4] and median is (2 + 3) / 2 = 2.5.
 *
 *
 *
 * Constraints:
 *
 *     nums1.length == m
 *     nums2.length == n
 *     0 <= m <= 1000
 *     0 <= n <= 1000
 *     1 <= m + n <= 2000
 *     -106 <= nums1[i], nums2[i] <= 106
 */
public class _4_Hard_MedianOfTwoSortedArrays
{
	public double findMedianSortedArrays(int[] nums1, int[] nums2) {
		//Current solution has a time complexity of O(m+n)
		//TODO : Need to work on a solution to bring it down to O(log(m+n)),
		// where I needn't have to iterate both the arrays and bring the time to half of the previous approach
		int m = nums1.length;
		int n = nums2.length;
		int[] nums3 = new int[m+n];
		int i=0,j=0,k=0;
		while(i<m || j<n)
		{
			if(i>=m || j>=n)
			{
				if(i<m)
				{
					nums3[k++] = nums1[i++];
				}
				else
				{
					nums3[k++] = nums2[j++];
				}
			}
			else
			{
				if(nums1[i]<nums2[j])
				{
					nums3[k++] = nums1[i++];
				}
				else
				{
					nums3[k++] = nums2[j++];
				}
			}
		}
		int o = nums3.length;
		double median;
		if(o%2==1)
		{
			median = nums3[o/2];
		}
		else
		{
			median = ((nums3[o/2] + nums3[(o/2)-1])/2.0);
		}
		return median;
	}
}