using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Euler15.Tests
{
    [TestClass]
    public class SumOfPowersTests
    {
        [TestMethod]
        public void GetPowerTest()
        {
            // Expected values
            int expectedValue5_8 = 390625;
            int expectedValue9_5 = 59049;
            int expectedValue7_5 = 16807;
            int expectedValue6_7 = 279936;
            int expectedValue4_9 = 262144;

            // Actual values
            int actualValue5_8 = SumOfPowers.GetPower(5, 8);
            int actualValue9_5 = SumOfPowers.GetPower(9, 5);
            int actualValue7_5 = SumOfPowers.GetPower(7, 5);
            int actualValue6_7 = SumOfPowers.GetPower(6, 7);
            int actualValue4_9 = SumOfPowers.GetPower(4, 9);


            // Asserts
            Assert.AreEqual(expectedValue5_8, actualValue5_8, "Error with base 5 and exponent 8");
            Assert.AreEqual(expectedValue9_5, actualValue9_5, "Error with base 9 and exponent 5");
            Assert.AreEqual(expectedValue7_5, actualValue7_5, "Error with base 7 and exponent 5");
            Assert.AreEqual(expectedValue6_7, actualValue6_7, "Error with base 6 and exponent 7");
            Assert.AreEqual(expectedValue4_9, actualValue4_9, "Error with base 4 and exponent 9");
        }
    }
}