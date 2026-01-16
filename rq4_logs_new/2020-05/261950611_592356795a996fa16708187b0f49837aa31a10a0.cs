using System;
using System.Collections.Generic;

namespace Leetcode
{
    class Program
    {
        public class Solution
        {
            public static int MinSteps(int n)
            {
                if (n == 1)
                {
                    return 0;
                }
                int a = 0;
                for (int i = 2; n > 1; i++)
                {
                    while (n % i == 0)
                    {
                        a += i;
                        n /= i;
                    }
                }
                return a;
            }

            public static void Main(string[] args)
            {
                Console.WriteLine(MinSteps(4));
            }
        }
    }
}