using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.Arrays
{
	public static class ArrayExcersices
	{

		//Height Checker
		//A school is trying to take an annual photo of all the students. The students are asked to stand in a single file line in non-decreasing order by
		//height. Let this ordering be represented by the integer array expected where expected[i] is the expected height of the ith student in line
		//You are given an integer array heights representing the current order that the students are standing in. Each heights[i] is the height of the ith student in line (0-indexed).
		//Return the number of indices where heights[i] != expected[i].
		public static int HeightChecker(int[] heights)
		{
			//Runtime: 80 ms
			//Memory Usage: 38.8 MB

			int[] expected = new int[heights.Length];
			int height = 0;
			expected = heights.OrderBy(x => x).ToArray();
			for (int i = 0; i < heights.Length; i++)
			{
				if (heights[i] != expected[i])
					height++;
			}
			return height;

			//Runtime: 64 ms

			//int[] expected = (int[])heights.Clone();
			//Array.Sort(expected);
			//int k = 0;

			//for (int i = 0; i < heights.Length; i++)
			//{
			//	if (heights[i] != expected[i])
			//		k++;
			//}
			//return k;
		}

		//Max Consecutive Ones II
		//Given a binary array nums, return the maximum number of consecutive 1's in the array if you can flip at most one 0.
		public static int FindMaxConsecutiveOnes(int[] nums)
		{
			//Runtime: 101  ms
			//Memory Usage: 52.4 MB

			int longSequence = 0;
			int left = 0, right = 0;
			int numZeros = 0;

			while (right < nums.Length)
			{
				if (nums[right] == 0)
					numZeros++;

				while (numZeros == 2)
				{
					if (nums[left] == 0)
					{
						numZeros--;
					}
					left++;
				}

				longSequence = Math.Max(longSequence, right - left + 1);
				right++;
			}

			return longSequence;
		}

		//Third Maximum Number
		//Given an integer array nums, return the third distinct maximum number in this array. If the third maximum does not exist, return the maximum number.
		public static int ThirdMax(int[] nums)
		{

			//Runtime: 90 ms
			//Memory Usage: 39.5 MB
			long first = long.MinValue, second = long.MinValue, third = long.MinValue;

			foreach (var num in nums)
			{
				if (num == first || num == second || num == third)
					continue;

				if (num > first)
				{
					third = second;
					second = first;
					first = num;
				}
				else if (num > second)
				{
					third = second;
					second = num;
				}
				else if (num> third)
				{
					third = num;
				}
			}

			return (int)(third == long.MinValue ? first :  third);
		}

	}
}