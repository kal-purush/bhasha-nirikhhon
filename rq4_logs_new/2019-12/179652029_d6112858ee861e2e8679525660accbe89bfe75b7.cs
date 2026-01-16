using System;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitMSTest
{
    /*You are climbing a stair case. It takes n steps to reach to the top.

    Each time you can either climb 1 or 2 steps.In how many distinct ways can you climb to the top?

    Note: Given n will be a positive integer.

    Example 1:

    Input: 2
    Output: 2
    Explanation: There are two ways to climb to the top.
    1. 1 step + 1 step
    2. 2 steps
    Example 2:

    Input: 3
    Output: 3
    Explanation: There are three ways to climb to the top.
    1. 1 step + 1 step + 1 step
    2. 1 step + 2 steps
    3. 2 steps + 1 step*/

    [TestClass]
    public class ClimbingStaris
    {
        public int ClimbStairs(int n)
        {
            if (n < 4) return n; 
            return ClimbStairs(n - 1) + ClimbStairs(n - 2);
        }

        public int ClimbStairs2(int n)
        {
            if (n < 4) return n;
            else
            {
                var fibonacciArray = new int[n + 1];

                fibonacciArray[1] = 1;
                fibonacciArray[2] = 2;
                for (int i = 3; i <= n; i++)
                {
                    fibonacciArray[i] = fibonacciArray[i - 1] + fibonacciArray[i - 2];
                }

                return fibonacciArray[n];
            }
        }


        [TestMethod]
        public void WorksCorrectly()
        {
            var result1 = ClimbStairs(2);
            var result2 = ClimbStairs(3);
            var result3 = ClimbStairs(4);
            var result4 = ClimbStairs(7);

            result1.Should().Be(2);
            result2.Should().Be(3);
            result3.Should().Be(5);
            result4.Should().Be(21);
        }
    }
}
 