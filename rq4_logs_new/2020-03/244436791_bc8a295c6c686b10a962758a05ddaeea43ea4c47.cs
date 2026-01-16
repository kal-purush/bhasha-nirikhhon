using Microsoft.VisualStudio.TestTools.UnitTesting;
using Square;
using System;
using System.Collections.Generic;
using System.Text;

namespace Square.Tests
{
    [TestClass()]
    public class SquaringTests
    {
        Squaring Square = new Squaring();
        private readonly int aInt = 1;
        private readonly int bInt = 2;
        private readonly double aDouble = 1.5;
        private readonly double bDouble = 2.5;

        [TestMethod()]
        public void SquareIntTest()
        {
            Square.SquareExpo(bInt);
            Assert.AreEqual(4, Squaring.Result);
        }
        [TestMethod()]
        public void SquareDoubleTest()
        {
            Square.SquareExpo(bDouble);
            Assert.AreEqual(6.50, Squaring.Result);
        }

      
    }
}