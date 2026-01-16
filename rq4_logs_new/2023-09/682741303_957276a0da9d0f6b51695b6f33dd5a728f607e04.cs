using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.Arrays
{
	public static class InPlaceArray
	{
		// In-Place Array Operations Introduction
		//Given an Array of integers, return an Array where every element at an even-indexed position is squared.
		public static int[] squareEven(int[] array, int length)
		{
			if (array == null)
				return null;

			int[] result = new int[length];

			for (int i = 0; i < length; i++)
			{
				int element = array[i];

				if (i % 2 == 0)
				{
					element *= element;
				}
				result[i] = element;
			}
			return result;
		}

		//Replace Elements with Greatest Element on Right Side
		//Given an array arr, replace every element in that array with the greatest element among the elements to its right, and replace the last element with -1.
		public static int[] ReplaceElements(int[] arr)
		{
			int max = arr[arr.Length - 1];
			int temp;
			for (int i = arr.Length - 1; i > -1; i--)// recorre el array comenzando por el final.
			{
				if (i == arr.Length - 1)
				{
					arr[i] = -1;
					continue;
				}
				if (arr[i] > max)
				{
					temp = arr[i];
					arr[i] = max;
					max = temp;
				}
				else if (arr[i] < max)
				{
					arr[i] = max;
				}
			}
			return arr;
		}

		//A Better Repeated Deletion Algorithm - Intro
		//Given a sorted array, remove the duplicates such that each element appears only once.
		public static int removeDuplicates(int[] nums)
		{

			// The initial length is simply the capacity.
			int length = nums.Length;

			// Assume the last element is always unique.
			// Then for each element, delete it iff it is
			// the same as the one after it. Use our deletion
			// algorithm for deleting from any index.
			for (int i = length - 2; i >= 0; i--)
			{
				if (nums[i] == nums[i + 1])
				{
					// Delete the element at index i, using our standard
					// deletion algorithm we learned.
					for (int j = i + 1; j < length; j++)
					{
						nums[j - 1] = nums[j];
					}
					// Reduce the length by 1.
					length--;
				}
			}
			// Return the new length.
			return length;
		}

		public static int[] copyWithRemovedDuplicates(int[] nums)
		{

			//check for edge cases.
			if (nums == null || nums.Length == 0)
				return nums;

			//Count how many unique elements are in the Array
			int uniqueNumber = 0;
			for (int i = 0; i < nums.Length; i++)
			{
				//An element should be counted as unique if it's the first
				//element in the Array, or is different to the one before it.
				if (i == 0 || nums[i] != nums[i - 1])
				{
					uniqueNumber++;
				}
			}
			//Create a result Array
			int[] result = new int[uniqueNumber];
			//Write the unique element into the result Array
			int positionInResult = 0;
			for (int i = 0; i < nums.Length; i++)
			{
				//same condition as in the previous loop. Except this time, we can wire
				//each unique number into the result Array instead of just counting them
				if (i == 0 || nums[i] != nums[i - 1])
				{
					result[positionInResult] = nums[i];
					positionInResult++;
				}
			}
			return result;
		}
	}
}