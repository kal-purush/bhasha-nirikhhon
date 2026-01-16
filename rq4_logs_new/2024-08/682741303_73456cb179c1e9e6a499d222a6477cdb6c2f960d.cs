using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.HashTable
{
	public class FindCommonElementsBetweenTwoArrays
	{
		public static int[] FindIntersectionValues(int[] nums1, int[] nums2)
		{
			HashSet<int> set1 = new HashSet<int>();
			HashSet<int> set2 = new HashSet<int>();
			for (int i = 0; i < nums1.Length; i++)
			{
				if (nums2.Contains(nums1[i]) && !set1.Contains(i))
				{
					set1.Add(i);
				}
			}
			for (int j = 0; j < nums2.Length; j++)
			{
				if (nums1.Contains(nums2[j]) && !set2.Contains(j))
				{
					set2.Add(j);
				}
			}
			return new int[] { set1.Count, set2.Count };
		}
	}
}