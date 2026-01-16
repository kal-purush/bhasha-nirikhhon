using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SimpleCalculator.Operations;

namespace SimpleCalculator.Tests
{
    [TestClass]
    public class ModulusTest
    {
        [TestMethod]
        public void EnsureFourModulusTwo()
        {
            ModulusizeNumber modulusizenumber = new ModulusizeNumber();
            int expectedResult = 0;
            int actualResult = modulusizenumber.ModulusizeStuff(4, 2);
            Assert.AreEqual(expectedResult, actualResult);
        }
    }
}