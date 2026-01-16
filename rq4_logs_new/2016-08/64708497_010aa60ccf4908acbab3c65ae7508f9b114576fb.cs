using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace Mathmagician.Tests
{
    [TestClass]
    public class PrimeTest
    {
        [TestMethod]
        public void Prime1()
        {
            Prime num = new Prime(1);
            List<int> expected = new List<int> { 2 };
            Assert.AreEqual(expected.ToString(), num.PrimeList().ToString());
        }
        [TestMethod]
        public void Prime2()
        {
            Prime num = new Prime(5);
            List<int> expected = new List<int> { 2, 3, 5, 7, 11 };
            Assert.AreEqual(expected.ToString(), num.PrimeList().ToString());
        }
    }
}