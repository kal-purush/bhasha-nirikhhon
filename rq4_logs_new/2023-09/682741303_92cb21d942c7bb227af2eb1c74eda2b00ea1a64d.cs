using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.ArrayString
{
	public static class IntroductionArray
	{
		//724. Find Pivot Index
		public static int PivotIndex(int[] nums)
		{
			//Runtime: 88 ms
			//Memory: 46.46 MB
			int sumLeft = 0, sumRight = nums.Sum();
			for (int i = 0; i < nums.Length; i++)
			{
				sumRight -= nums[i];
				if (sumLeft == sumRight)
					return i;

				sumLeft += nums[i];
			}
			return -1;
		}

		public static int DominantIndex(int[] nums)
		{
			//Runtime: 75 ms
			//Memory: 38.9 MB
			int maxIdex = nums.Max();
			for (int i = 0; i < nums.Length; i++)
			{
				if (nums[i] != maxIdex && nums[i] * 2 > maxIdex)
					return -1;
			}
			return Array.IndexOf(nums, maxIdex);

		}

		//66. Plus One
		//You are given a large integer represented as an integer array digits, where each digits[i] is the ith digit of the integer. The digits are
		//ordered from most significant to least significant in left-to-right order. The large integer does not contain any leading 0's.
		//Increment the large integer by one and return the resulting array of digits
		//Example 1:

		//Input: digits = [1,2,3]
		//Output: [1,2,4]
		//Explanation: The array represents the integer 123.
		//Incrementing by one gives 123 + 1 = 124.
		//Thus, the result should be [1,2,4].
		public static int[] PlusOne(int[] digits)
		{
			//Runtime: 115 ms
			//Memory: 42.6 MB

			//move along the input array starting from the end
			for (int i = digits.Length - 1; i >= 0; --i)
			{
				if (digits[i] == 9)
				{
					digits[i] = 0;
				}
				else
				{
					digits[i]++;
					return digits;
				}

			}
			//we are here because all the digits are nines
			digits = new int[digits.Length + 1];
			digits[0] = 1;

			return digits;
		}
	}
}