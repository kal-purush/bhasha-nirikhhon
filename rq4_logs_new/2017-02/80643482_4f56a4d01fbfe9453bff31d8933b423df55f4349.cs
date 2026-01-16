using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SimpleCalculator.StoredConstants;

namespace SimpleCalculatorTests
{
    [TestClass]
    public class StoredConstantsTest
    {
        [TestMethod]
        public void EnsureInDictionary()
        {
            StoredConstants constants = new StoredConstants();
            constants.AddConstantsToDictionary();
            bool expectedResult = true;
            bool actualResult = constants.IsValueInDictionary('c');
            Assert.AreEqual(expectedResult, actualResult);

        }
    }
}