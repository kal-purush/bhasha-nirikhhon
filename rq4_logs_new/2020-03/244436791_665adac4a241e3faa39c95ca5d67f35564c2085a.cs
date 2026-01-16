using Microsoft.VisualStudio.TestTools.UnitTesting;
using Multiplication;
using System;
using System.Collections.Generic;
using System.Text;

namespace Multiplication.Tests
{
    [TestClass()]
    public class MultiplyingTests
    {
        Multiplying Multiplication = new Multiplying();
        private readonly int aInt = 1;
        private readonly int bInt = 2;
        private readonly double aDouble = 1.5;
        private readonly double bDouble = 2.5;

        [TestMethod()]
        public void ProductIntTest()
        {
            Multiplication.Product(aInt, bInt);
            Assert.AreEqual(2, Multiplying.Result);
        }

        [TestMethod()]
        public void ProductDoubleTest()
        {
            Multiplication.Product(aDouble, bDouble);
            Assert.AreEqual(3.75, Multiplying.Result);
        }
    }
}