using System;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitMSTest
{
    
    /*Given two binary strings, return their sum(also a binary string).

    The input strings are both non-empty and contains only characters 1 or 0.

    Example 1:

    Input: a = "11", b = "1"
    Output: "100"
    Example 2:

    Input: a = "1010", b = "1011"
    Output: "10101"*/

    [TestClass]
    public class AddBinary
    {
        public string AddBin(string a, string b)
        {

            return String.Empty;
        }


        [TestMethod]
        public void WorksCorrectly()
        {
            var input1 = new string[] {"11", "1"};
            var input2 = new string[] {"1010", "1011"};

            var result1 = AddBin(input1[0], input1[1]);
            var result2 = AddBin(input2[0], input2[1]);

            //result1.Should().Be();
            //result2.Should().Be();
        }
    }
}