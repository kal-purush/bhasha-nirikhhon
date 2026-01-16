using System;
using System.Collections.Generic;

namespace Leetcode
{
    class Program
    {
        public class Solution
        {
            public static bool Find132pattern(int[] nums)
            {
                List<int[]> interval = new List<int[]>();
                if (nums.Length != 0)
                {
                    //if the list is not null
                    int temp = nums[0];
                    for (int i = 1; i < nums.Length; i++)
                    {
                        if (nums[i] < nums[i - 1])
                        {
                            //if there is a down slope which means before this point
                            //there may be a up slope interval
                            if (nums[i - 1] > temp)
                            {
                                //if there is a up slope interval
                                interval.Add(new int[] { temp, nums[i - 1] });
                            }
                            //update the start point of next interval
                            temp = nums[i];
                        }
                        foreach (int[] m in interval)
                        {
                            //if there is a number during[i,length] suitable for 132 pattern
                            if (nums[i] > m[0] && nums[i] < m[1])
                            {
                                return true;
                            }
                        }
                    }
                }
                return false;
            }

            public static void Main(string[] args)
            {
                int[] test = new int[] { 1, 1, 0 };
                Console.WriteLine(Find132pattern(test));
            }
        }
    }
}