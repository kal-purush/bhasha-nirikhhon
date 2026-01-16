using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.Arrays
{
	public static class ArrayPracticExcersice
	{
		//1512. Number of Good Pairs
		//Given an array of integers nums, return the number of good pairs.
		//A pair(i, j) is called good if nums[i] == nums[j] and i<j.

		//Example 1:
		//Input: nums = [1,2,3,1,1,3]
		//Output: 4
		//Explanation: There are 4 good pairs(0,3), (0,4), (3,4), (2,5) 0-indexed.
		public static int NumIdenticalPairs(int[] nums)
		{
			int ans = 0;
			for (int i = 0; i < nums.Length; i++)
			{
				for (int j = i + 1; j < nums.Length; j++)
				{
					if (nums[i] == nums[j])// se compara uno a uno entre las dos listas,
										   // luego se acumula el resultado de la comparacion entre las listas
						ans++;

				}
			}
			return ans;
		}

		//2798. Number of Employees Who Met the Target
		//There are n employees in a company, numbered from 0 to n - 1. Each employee i has worked for hours[i] hours in the company.
		//The company requires each employee to work for at least target hours.
		//You are given a 0-indexed array of non-negative integers hours of length n and a non-negative integer target.
		//Return the integer denoting the number of employees who worked at least target hours.
		public static int NumberOfEmployeesWhoMetTarget(int[] hours, int target)
		{
			//Runtime: 86 ms
			//Memory: 40.53 MB
			int ans = 0;
			for (int i = 0; i < hours.Length; i++)
			{
				if (hours[i] >= target)
					ans++;				
			}
			return ans;
		}
	}
}