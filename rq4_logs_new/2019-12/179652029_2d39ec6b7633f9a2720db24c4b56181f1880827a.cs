using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;

namespace UnitMSTest
{

    /*Determine whether an integer is a palindrome.An integer is a palindrome when it reads the same backward as forward.

    Example 1:

    Input: 121
    Output: true
    Example 2:

    Input: -121
    Output: false
    Explanation: From left to right, it reads -121. From right to left, it becomes 121-. Therefore it is not a palindrome.
    Example 3:

    Input: 10
    Output: false
    Explanation: Reads 01 from right to left. Therefore it is not a palindrome.*/

    [TestClass]
    public class PalindromeNumber
    {
        //approach if (arr[x] != arr[arr.length-x-1]) return // not a palindrome
        //approach compare only half of the integer

        public bool IsPalindrome(int x)
        {
            if (x < 0) return false;
            else if (x == 0) return true;
            else if (x % 10 == 0) return false;

            var inputNumber = x;
            var reversedNumber = 0;
            while (x > 0)
            {
                reversedNumber = reversedNumber * 10 + x % 10;
                x = x / 10;
            }

            return (reversedNumber == inputNumber);
        }

        [TestMethod]
        public void WorksCorretly()
        {
            var number1 = 121;
            var number2 = 0;
            var number3 = 120;
            var number4 = -120;

            var result1 = IsPalindrome(number1);
            var result2 = IsPalindrome(number2);
            var result3 = IsPalindrome(number3);
            var result4 = IsPalindrome(number4);

            result1.Should().Be(true);
            result2.Should().Be(true);
            result3.Should().Be(false);
            result4.Should().Be(false);
        }

        [TestMethod]
        public void WorksForLongNumbers()
        {
            var number1 = 123555321;
            var number2 = 1345125431;

            var result1 = IsPalindrome(number1);
            var result2 = IsPalindrome(number2);

            result1.Should().Be(true);
            result2.Should().Be(false);
        }
    }
}